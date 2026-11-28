package lifecycle_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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
		RenderDigest: "render", EnvDigest: "env", Images: []string{"img"}, ResourceIDs: []string{"deployment/app"}, Ready: true, AcceptedCommit: "deployed-commit",
	}}
}

func refreshConfig() lifecycle.Config {
	return lifecycle.Config{SettingsDigest: "settings-digest", Application: "app", Namespace: "ns", InfrastructureTries: 1, PendingEvaluations: 1}
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

// A seeded install SDO never deployed has no recorded deployment, but it has a
// live application: the judge's staged candidate is verified against it (the
// deployment items skipped, never run), and the verified proposal is published.
func TestRefreshHealthOfASeededInstallVerifiesTheDetectorsLive(t *testing.T) {
	h, fake := gateHarness(t, clear)
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || fake.stages != 1 || fake.publishes != 1 || fake.accepts != 0 {
		t.Fatalf("receipt=%+v stages=%d publishes=%d accepts=%d, want the staged candidate verified, then published", receipt, fake.stages, fake.publishes, fake.accepts)
	}
	if want := fake.candidates[0].Path + "@" + fake.candidates[0].Commit; len(h.verifier.candidateRoots) != 1 || h.verifier.candidateRoots[0] != want {
		t.Fatalf("detectors verified on %v, want the staged checkout %s", h.verifier.candidateRoots, want)
	}
	if source := h.verifier.sources[0]; !source.Seeded || source.Ready {
		t.Fatalf("verified against %+v, want the seeded application as found", source)
	}
	if len(h.readiness.liveCalls) != 0 {
		t.Fatalf("deployment checks ran on %v; a seeded install has no recorded deployment to check", h.readiness.liveCalls)
	}
}

// A user test the designer adds that fails on the healthy seeded application
// (it never passed there) goes back to the designer with the failing probe's
// file, and only the corrected proposal is published.
func TestRefreshHealthOfASeededInstallReturnsAFailingProbeToTheDesigner(t *testing.T) {
	probeFails := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: "accepted health detectors report firing", Detectors: []lifecycle.DetectorState{{DetectorID: "traffic-writes", Status: "firing", Reason: "write-commit: step push-event failed"}}}
	h, fake := gateHarness(t, probeFails, clear)
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || h.workers.judgeRuns != 2 || fake.publishes != 1 || fake.discards != 2 {
		t.Fatalf("receipt=%+v judge runs=%d publishes=%d discards=%d, want one correction, then one publication", receipt, h.workers.judgeRuns, fake.publishes, fake.discards)
	}
	previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
	if len(previous) == 0 || previous[0].GetRule() != "operability.O4.detectors" ||
		previous[0].GetPath() != ".sdo/diagnostics/traffic/workloads/writes.yaml" || !strings.Contains(previous[0].GetMessage(), "push-event") {
		t.Fatalf("designer correction diagnostics = %v", previous)
	}
}

// A designer probe that keeps failing on the seeded application is never
// re-attributed to the application and never published: the bounded judge
// loop ends the refresh instead.
func TestRefreshHealthOfASeededInstallNeverPublishesAProbeThatKeepsFailing(t *testing.T) {
	h, fake := gateHarness(t, judgeFiring)
	if _, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision()); err == nil {
		t.Fatal("a refresh whose designer probe never passed on the live application succeeded")
	}
	if fake.publishes != 0 || fake.accepts != 0 || h.workers.judgeRuns < 2 {
		t.Fatalf("publishes=%d accepts=%d judge runs=%d, want corrections and no publication", fake.publishes, fake.accepts, h.workers.judgeRuns)
	}
}

// When a detector Go wrote fires, the seeded application itself is unhealthy
// and nothing can judge the designer's detectors: the refresh is published
// unverified, as before, rather than keeping the controller from starting.
func TestRefreshHealthOfAnUnhealthySeededInstallPublishesUnverified(t *testing.T) {
	h, fake := gateHarness(t, firing)
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || fake.publishes != 1 || h.workers.judgeRuns != 1 {
		t.Fatalf("receipt=%+v publishes=%d judge runs=%d, want one unverified publication", receipt, fake.publishes, h.workers.judgeRuns)
	}
}

// A seeded refresh is verified live, so a Coordinator without a verifier
// refuses before any judge work.
func TestRefreshHealthOfASeededInstallNeedsAVerifier(t *testing.T) {
	h, _ := gateHarness(t, clear)
	h.coordinator.Verifier = nil
	_, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	var missing *lifecycle.MissingDependencyError
	if !errors.As(err, &missing) || h.workers.judgeRuns != 0 {
		t.Fatalf("error = %v, judge runs = %d; want a MissingDependencyError before any judge", err, h.workers.judgeRuns)
	}
}

// The controller's refresh carries the accepted objective in the revision, not
// in its Config: the candidate is verified against that objective. Without it
// every refresh of a deployed application failed its verification and the
// controller crash-looped on start.
func TestRefreshHealthVerifiesAgainstTheAcceptedObjective(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.source = deployedSource()
	cfg := refreshConfig()
	cfg.HealthObjective = ""
	if _, err := h.coordinator.RefreshHealth(context.Background(), cfg, refreshRevision()); err != nil {
		t.Fatal(err)
	}
	if len(h.verifier.objectives) != 1 || h.verifier.objectives[0] != refreshRevision().Objective {
		t.Fatalf("verified objectives = %q, want the accepted %q", h.verifier.objectives, refreshRevision().Objective)
	}
}

// A push that landed after the last deployment is a head to deploy, not a
// change the deployed application can vouch for: judging the pushed tree
// against the previous deployment's receipt failed O2.resources (the receipt
// held the old release's generated Job, the desired set the new one), so the
// controller crash-looped on start and the head poller, which lives in that
// controller, never deployed the push. The live application is not the head
// either, so the refresh publishes unverified and the head deployment verifies
// the push.
func TestRefreshHealthOfAHeadAheadOfItsDeploymentPublishesWithoutLiveVerification(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.source = deployedSource()
	fake.headUndeployed = true
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Commit == "" || fake.stages != 0 || fake.accepts != 0 || h.verifier.calls != 0 || len(h.readiness.liveCalls) != 0 {
		t.Fatalf("receipt=%+v stages=%d accepts=%d verifications=%d live=%v, want a plain publication verified against nothing", receipt, fake.stages, fake.accepts, h.verifier.calls, h.readiness.liveCalls)
	}
}

// A refresh re-judges an application already onboarded: its existing-records
// requirement was met when the map was accepted, and the verdict documents the
// evidence as gating onboarding acceptance only. A restarted controller
// evaluates before (or without) the probe's existing-records sample, so gating
// the refresh on it crash-looped the controller on a healthy application
// ("O4.existing-records ... still firing after 2 designer corrections") and
// kept the next push from ever being deployed.
func TestRefreshHealthDoesNotGateOnExistingRecords(t *testing.T) {
	unexercised := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationClear, Summary: "accepted health detectors report clear", ExistingRecords: lifecycle.ExistingRecordsEvidence{Required: true}}
	h, fake := gateHarness(t, unexercised)
	fake.source = deployedSource()
	receipt, err := h.coordinator.RefreshHealth(context.Background(), refreshConfig(), refreshRevision())
	if err != nil || receipt.Commit == "" || fake.accepts != 1 {
		t.Fatalf("receipt=%+v accepts=%d err=%v: a clear refresh was refused for an unexercised existing-records step", receipt, fake.accepts, err)
	}
}

// An evaluation that observes for minutes spends the pending window: a refresh
// whose probes stay pending answers after the window's elapsed time, not after
// the full count of evaluations, each minutes long (8 x 2.5 minutes on a cold
// prober kept a gated refresh from ever answering within its budget).
func TestPendingWindowIsElapsedTimeNotOnlyACount(t *testing.T) {
	h, _ := gateHarness(t, pending)
	clock := time.Unix(1000, 0)
	h.coordinator.Now = func() time.Time { clock = clock.Add(3 * time.Minute); return clock }
	cfg := refreshConfig()
	cfg.PendingEvaluations, cfg.PendingInterval = 8, 15*time.Second
	_, _ = h.coordinator.RefreshHealth(context.Background(), cfg, refreshRevision())
	if h.verifier.calls == 0 || h.verifier.calls > 2*h.workers.judgeRuns {
		t.Fatalf("verifications = %d over %d judge runs, want at most two per candidate once the 2-minute window is spent", h.verifier.calls, h.workers.judgeRuns)
	}
}
