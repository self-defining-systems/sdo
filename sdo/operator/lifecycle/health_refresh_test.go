package lifecycle_test

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

func refreshRevision() memory.HealthRevision {
	return memory.HealthRevision{Commit: "commit-0", JudgeCommit: "old", SourceInputs: "inputs", Objective: "stay up", DetectorRevision: "rev", NeedsRefresh: true}
}

func deployedSource() *broker.AcceptedSource {
	return &broker.AcceptedSource{Evidence: broker.SourceEvidence{
		DeploymentSource: &contracts.DeploymentSource{}, BaseTree: "tree", EffectiveTree: "effective", PatchesDigest: "patches", ObservedRevision: "effective",
		RenderDigest: "render", EnvDigest: "env", Images: []string{"img"}, ResourceIDs: []string{"deployment/app"}, Ready: true,
	}}
}

func refreshConfig() lifecycle.Config {
	return lifecycle.Config{Application: "app", Namespace: "ns", InfrastructureTries: 1, PendingEvaluations: 1}
}

// A refresh gates the judge's candidate against the live deployment, so a
// Coordinator built without a deployer or verifier refuses with a typed error
// naming what is missing, before any judge work and never with a panic.
func TestRefreshHealthNamesTheMissingDeployerAndVerifier(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	h.coordinator.Readiness, h.coordinator.Verifier = nil, nil
	h.coordinator.Broker.(*fakeBroker).source = deployedSource()
	_, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	var missing *lifecycle.MissingDependencyError
	if !errors.As(err, &missing) || len(missing.Missing) != 2 || h.workers.judgeRuns != 0 {
		t.Fatalf("error = %v, judge runs = %d; want a MissingDependencyError for Readiness and Verifier before any judge", err, h.workers.judgeRuns)
	}
}

// The refresh verifies the judge's candidate against the accepted head's
// recorded deployment, re-verified live: the receipt is complete and ready.
func TestRefreshHealthGatesTheCandidateAgainstTheAcceptedDeployment(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.source = deployedSource()
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || fake.accepts != 1 || h.verifier.calls != 1 {
		t.Fatalf("receipt=%+v accepts=%d verifications=%d, want the candidate verified live and accepted", receipt, fake.accepts, h.verifier.calls)
	}
	if len(h.readiness.liveCalls) != 1 || h.readiness.liveCalls[0] != fake.candidates[0].Path {
		t.Fatalf("live checks ran on %v, want the staged checkout", h.readiness.liveCalls)
	}
}

// A seeded install SDO never deployed has no recorded deployment to verify the
// candidate against: the refresh publishes through the broker's own validation,
// stages and verifies nothing live, and the typed unverified path is the only
// way to get there.
func TestRefreshHealthOfASeededInstallPublishesWithoutLiveVerification(t *testing.T) {
	h, fake := gateHarness(t, clear)
	h.coordinator.Readiness, h.coordinator.Verifier = nil, nil
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || fake.stages != 0 || fake.accepts != 0 {
		t.Fatalf("receipt=%+v stages=%d accepts=%d, want a plain publication with no staged verification", receipt, fake.stages, fake.accepts)
	}
}
