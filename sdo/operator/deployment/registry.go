// Package deployment runs trusted, registered source deployment strategies.
// Agent plans select a strategy, but only its Go implementation can attest a
// candidate tree and the observed running revision.
package deployment

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
)

type Observation struct {
	ContainerImages []rendered.DeployedImage
	Revision        string
	Images          []string
	ResourceIDs     []string
	Ready           bool
}

type Registration struct {
	ID     string
	Config proto.Message
	Apply  func(context.Context, string, string, proto.Message) (Observation, error)
	Verify func(context.Context, string, string, lifecycle.DeploymentReceipt) (Observation, error)
}

type Registry struct {
	AcceptedRoot string
	entries      map[string]Registration
}

// VerifyAccepted rechecks the persisted deployment evidence after a restart.
// The source commit must be in the accepted branch and have the exact tree
// whose workloads the strategy originally deployed.
func (registry *Registry) VerifyAccepted(ctx context.Context, acceptedCommit, namespace string, prior lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	if registry == nil || registry.AcceptedRoot == "" || namespace == "" || acceptedCommit == "" || !prior.Ready || prior.ProposalTreeDigest == "" {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted repository, commit, namespace, and trusted source evidence are required")
	}
	entry, found := registry.entries[prior.StrategyID]
	if !found || entry.Verify == nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("deployment strategy %q cannot reverify accepted source", prior.StrategyID)
	}
	git := func(args ...string) (string, error) {
		output, err := exec.CommandContext(ctx, "git", append([]string{"-C", registry.AcceptedRoot}, args...)...).CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("accepted git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if _, err := git("merge-base", "--is-ancestor", acceptedCommit, head); err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("source commit is not in the accepted branch: %w", err)
	}
	tree, err := git("rev-parse", acceptedCommit+"^{tree}")
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if tree != prior.ProposalTreeDigest || prior.ObservedRevision != tree {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted source tree differs from persisted deployment evidence")
	}
	observed, err := entry.Verify(ctx, acceptedCommit, namespace, prior)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if !observed.Ready || observed.Revision != tree || !equalStrings(observed.Images, prior.Images) || !equalStrings(observed.ResourceIDs, prior.ResourceIDs) {
		return lifecycle.DeploymentReceipt{}, errors.New("current workload evidence differs from persisted source deployment")
	}
	return prior, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
	}
	for _, value := range counts {
		if value != 0 {
			return false
		}
	}
	return true
}

var strategyID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
var digestReference = regexp.MustCompile(`^.+@sha256:[a-f0-9]{64}$`)

func NewRegistry(entries ...Registration) (*Registry, error) {
	registry := &Registry{entries: map[string]Registration{}}
	for _, entry := range entries {
		if !strategyID.MatchString(entry.ID) || entry.Config == nil || entry.Apply == nil {
			return nil, fmt.Errorf("invalid deployment strategy registration %q", entry.ID)
		}
		if _, exists := registry.entries[entry.ID]; exists {
			return nil, fmt.Errorf("duplicate deployment strategy %q", entry.ID)
		}
		registry.entries[entry.ID] = entry
	}
	return registry, nil
}

// ApplyAndVerify computes the exact candidate tree using a temporary Git
// index. It never trusts a tree digest or readiness assertion from the worker.
func (registry *Registry) ApplyAndVerify(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	if registry == nil || plan == nil {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment registry and plan are required")
	}
	entry, found := registry.entries[plan.GetStrategyId()]
	if !found {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("unregistered deployment strategy %q", plan.GetStrategyId())
	}
	if plan.GetStrategyConfig() == nil {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment strategy config is required")
	}
	config := proto.Clone(entry.Config)
	if err := plan.StrategyConfig.UnmarshalTo(config); err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("decode deployment strategy %q: %w", entry.ID, err)
	}
	tree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	observed, err := entry.Apply(ctx, checkout, tree, config)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("deployment strategy %q: %w", entry.ID, err)
	}
	if observed.Revision != tree || !observed.Ready || len(observed.Images) == 0 || len(observed.ResourceIDs) == 0 {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment strategy returned incomplete or mismatched readiness evidence")
	}
	for _, image := range observed.Images {
		if !digestReference.MatchString(image) {
			return lifecycle.DeploymentReceipt{}, fmt.Errorf("deployment image is not immutable: %q", image)
		}
	}
	for _, resource := range observed.ResourceIDs {
		if resource == "" {
			return lifecycle.DeploymentReceipt{}, errors.New("deployment resource identity is empty")
		}
	}
	receipt := lifecycle.DeploymentReceipt{
		ContainerImages: observed.ContainerImages, ProposalTreeDigest: tree, ObservedRevision: observed.Revision, StrategyID: entry.ID,
		Images: observed.Images, ResourceIDs: observed.ResourceIDs, Ready: true,
	}
	if raw, ok := config.(*contracts.RawManifestStrategy); ok {
		receipt.ManifestPaths = append([]string(nil), raw.GetManifestPaths()...)
	}
	return receipt, nil
}
