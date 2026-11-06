// Package deployment runs trusted, registered source deployment strategies.
// Agent plans select a strategy, but only its Go implementation can attest a
// candidate tree and the observed running revision.
//
//archcheck:allow R4 until=S3-13b reason="generic registry switches on the raw strategy"
//archcheck:ratchet R5 count=1 until=S3-13b
package deployment

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sdo.dev/operator/deployment/internal/deploymentio"
	"strings"

	"google.golang.org/protobuf/proto"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
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
	// Renderers and Env digest what a deployment rendered (the same
	// rendered.Desired the controller reads with); Env.Namespace is the
	// application namespace. Secrets is chosen per operation.
	Renderers *render.Catalog
	Env       render.Env
	entries   map[string]Registration
}

// VerifyAccepted rechecks the persisted deployment evidence after a restart.
// The source commit must be in the accepted branch and have the exact tree
// whose workloads the strategy originally deployed, and the recorded source
// must still render to the recorded digests (*RendererChangedError).
func (registry *Registry) VerifyAccepted(ctx context.Context, acceptedCommit, namespace string, prior lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	if registry == nil || registry.AcceptedRoot == "" || namespace == "" || acceptedCommit == "" || !prior.Ready || prior.BaseTree == "" {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted repository, commit, namespace, and trusted source evidence are required")
	}
	entry, found := registry.verifier(prior.Source)
	if !found {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("no deployment strategy can reverify accepted source with renderer %q", prior.Source.GetRendererId())
	}
	if namespace != registry.Env.Namespace {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted source namespace differs from the registry's namespace")
	}
	git := func(ctx context.Context, args ...string) (string, error) {
		output, err := deploymentio.Run(ctx, "git", append([]string{"-C", registry.AcceptedRoot}, args...), nil)
		if err != nil {
			return "", fmt.Errorf("accepted git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
		}
		return strings.TrimSpace(string(output)), nil
	}
	tree, err := acceptedTree(ctx, git, acceptedCommit, prior)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if err := verifyRendered(ctx, rendered.ObjectStore{Dir: registry.AcceptedRoot}, tree, registry.env(render.SecretsOmit), registry.Renderers, prior); err != nil {
		return lifecycle.DeploymentReceipt{}, err
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

func (registry *Registry) env(mode render.SecretMode) render.Env {
	env := registry.Env
	env.Secrets = mode
	return env
}

// verifier finds the registration that re-observes a deployment of source.
// Only the raw strategy remains; its sources use the raw renderer.
func (registry *Registry) verifier(source *contracts.DeploymentSource) (Registration, bool) {
	if source.GetRendererId() != rawRendererID {
		return Registration{}, false
	}
	for _, entry := range registry.entries {
		if _, raw := entry.Config.(*contracts.RawManifestStrategy); raw && entry.Verify != nil {
			return entry, true
		}
	}
	return Registration{}, false
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

// rawRendererID is the renderer whose sources the raw strategy deploys.
const rawRendererID = "raw"

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
	raw, ok := config.(*contracts.RawManifestStrategy)
	if !ok {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("deployment strategy %q has no deployment source", entry.ID)
	}
	source, err := rendered.LegacyRawSource(raw.GetManifestPaths())
	if err != nil || source == nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("raw deployment source: %v", err)
	}
	tree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	// Digest before any cluster effect: a source the shared desired-state
	// function cannot render is never deployed.
	set, err := renderedDigests(ctx, rendered.ObjectStore{Dir: checkout}, tree, registry.env(render.SecretsOmit), registry.Renderers, source)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("render deployment source: %w", err)
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
	return lifecycle.DeploymentReceipt{
		ContainerImages: observed.ContainerImages, Source: source, BaseTree: tree, ObservedRevision: observed.Revision,
		RenderDigest: set.RenderDigest, EnvDigest: set.EnvDigest, Images: observed.Images, ResourceIDs: observed.ResourceIDs, Ready: true,
	}, nil
}
