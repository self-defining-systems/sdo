package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/planfile"
)

// RefreshHealth reuses judge publication without redeployment, architecture
// authorship, or bootstrap. The broker's proposal/acceptance and rejection log
// use an input-bound ID, so interruption resumes the same publication.
func (c Coordinator) RefreshHealth(ctx context.Context, cfg Config, revision memory.HealthRevision) (broker.Receipt, error) {
	if c.Broker == nil || c.Bootstrap == nil || c.Registry == nil || c.Workers == nil || cfg.Application == "" || cfg.Namespace == "" || revision.Commit == "" || revision.SourceInputs == "" || revision.DetectorRevision == "" || revision.Objective == "" {
		return broker.Receipt{}, errors.New("health refresh requires broker, judge, application and accepted health inputs")
	}
	if !revision.NeedsRefresh {
		return broker.Receipt{Commit: revision.Commit}, nil
	}
	// The judge's candidate is verified against the live deployment of the
	// accepted source. A seeded install that SDO never deployed has none: its
	// candidate is published unverified, and said so (see publishUnverified).
	deployment, err := c.acceptedDeployment(ctx, cfg, revision.Commit)
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	// The logical input ID ignores operational-memory commits. The concrete
	// proposal/task must also bind its immutable accepted base: a retained Job
	// must never receive a different request or write into a recreated checkout.
	publicationID, err := c.refreshGeneration(refreshPublicationID(revision, cfg.RefreshScope))
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	receipt, ready, feedback, err := c.judge(ctx, publicationID, cfg, 1, revision.Commit, revision.Commit, "", deployment)
	if err != nil {
		return broker.Receipt{}, fmt.Errorf("refresh health requirements: %w", err)
	}
	if !ready {
		return broker.Receipt{}, fmt.Errorf("health unverified: %s", feedback)
	}
	return receipt, nil
}

// refreshGeneration returns the publication ID whose rejection log still has
// attempts left. A log made terminal only by lost workers says nothing about
// the judge's work, so the next controller start gets a fresh bounded
// generation instead of failing on that log forever.
func (c Coordinator) refreshGeneration(base string) (string, error) {
	for generation := 0; ; generation++ {
		id := base
		if generation > 0 {
			id = fmt.Sprintf("%s-g%d", base, generation)
		}
		log, err := c.Broker.Rejections(id)
		if err != nil {
			return "", err
		}
		if !log.GetTerminal() || !onlyLostWorkers(log) {
			return id, nil
		}
	}
}

func onlyLostWorkers(log *contracts.ProposalRejectionLog) bool {
	for _, rejection := range log.GetRejections() {
		diagnostics := rejection.GetDiagnostics()
		if len(diagnostics) == 0 || diagnostics[0].GetRule() != vanishedWorkerRule {
			return false
		}
	}
	return true
}

// RefreshScopeAdvance is the scope of the refresh an upstream advance runs
// after it accepted a shape change.
const RefreshScopeAdvance = "advance"

// refreshPublicationID is the base publication ID of a refresh: its inputs
// bound to the accepted commit, plus the scope of whoever runs it.
func refreshPublicationID(revision memory.HealthRevision, scope string) string {
	id := revision.RefreshID() + "-base-" + revision.Commit
	if scope != "" {
		id += "-" + scope
	}
	return id
}

// MissingDependencyError names the collaborators an operation needs that the
// Coordinator was built without. It is returned instead of panicking on a nil
// interface.
type MissingDependencyError struct {
	Operation string
	Missing   []string
}

func (e *MissingDependencyError) Error() string {
	return fmt.Sprintf("%s requires %s", e.Operation, strings.Join(e.Missing, ", "))
}

func missingNames(absent map[string]bool) []string {
	var names []string
	for name, missing := range absent {
		if missing {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// acceptedDeployment re-verifies the recorded deployment of the accepted head
// against the live cluster and returns the receipt a refreshed detector is
// judged against. With no recorded deployment it returns the zero receipt (not
// Ready), the signal for the unverified publication of a seeded install.
func (c Coordinator) acceptedDeployment(ctx context.Context, cfg Config, commit string) (DeploymentReceipt, error) {
	accepted, found, err := c.Broker.AcceptedSourceEvidence(ctx)
	if errors.Is(err, planfile.ErrAbsent) {
		found, err = false, nil
	}
	if err != nil {
		return DeploymentReceipt{}, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if !found {
		return DeploymentReceipt{Source: cfg.DeploymentSource, Environment: cfg.RenderEnvironment}, nil
	}
	if c.Readiness == nil || c.Verifier == nil {
		return DeploymentReceipt{}, &MissingDependencyError{Operation: "health refresh of a deployed application", Missing: missingNames(map[string]bool{"Readiness": c.Readiness == nil, "Verifier": c.Verifier == nil})}
	}
	recorded := deploymentFromBroker(accepted.Evidence)
	live, err := c.Readiness.VerifyAccepted(ctx, commit, cfg.Namespace, recorded)
	if err != nil {
		return DeploymentReceipt{}, fmt.Errorf("verify accepted source: %w", refusedByPolicy(err, commit))
	}
	if !completeDeploymentReceipt(live) || !sameDeploymentIdentity(recorded, live) {
		return DeploymentReceipt{}, errors.New("accepted source has incomplete or changed deployment evidence")
	}
	return live, nil
}
