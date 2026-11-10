package lifecycle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operability"
)

// failLive is the live deployment checks with check id failing for cause.
func failLive(id string, cause operability.Cause) []operability.Check {
	var checks []operability.Check
	for _, name := range []string{"O1.deploy", "O1.ready", "O2.stamps", "O6.budget"} {
		check := operability.Check{ID: name, Phase: operability.PhaseLive, Status: operability.StatusPass}
		if name == id {
			check.Status, check.Cause, check.Detail = operability.StatusFail, cause, "synthetic "+string(cause)
		}
		checks = append(checks, check)
	}
	return checks
}

func passLive() []operability.Check { return failLive("", "") }

func gateHarness(t *testing.T, verdicts ...lifecycle.HealthVerdict) (*harness, *fakeBroker) {
	t.Helper()
	h := newHarness(t, &fakeVerifier{verdicts: verdicts})
	return h, h.coordinator.Broker.(*fakeBroker)
}

// The judge's candidate is staged, verified where it is staged, and only then
// accepted; nothing verifies the accepted head afterwards.
func TestCleanCandidateIsVerifiedStagedThenAccepted(t *testing.T) {
	h, fake := gateHarness(t, clear)
	result, err := h.coordinator.Run(context.Background(), config())
	if err != nil {
		t.Fatal(err)
	}
	if fake.stages != 1 || fake.accepts != 1 || fake.discards != 0 {
		t.Fatalf("stages=%d accepts=%d discards=%d", fake.stages, fake.accepts, fake.discards)
	}
	candidate := fake.candidates[0]
	if want := candidate.Path + "@" + candidate.Commit; len(h.verifier.candidateRoots) != 1 || h.verifier.candidateRoots[0] != want {
		t.Fatalf("VerifyCandidate ran on %v, want the staged checkout %s", h.verifier.candidateRoots, want)
	}
	// The first live call is the deployment's (stage deployment), the second the
	// staged candidate's (stage complete).
	if len(h.readiness.liveCalls) != 2 || h.readiness.liveCalls[1] != candidate.Path {
		t.Fatalf("live checks ran on %v, want the deployment, then the staged checkout", h.readiness.liveCalls)
	}
	evidence := fake.evidence2[0]
	if evidence.Commit != candidate.Commit || evidence.EffectiveTree != "effective" || evidence.PatchesDigest != "patches" {
		t.Fatalf("accept evidence %+v does not bind the candidate to the deployed effective tree", evidence)
	}
	if h.verifier.calls != 1 {
		t.Fatalf("verifier calls = %d: the accepted head must not be verified again", h.verifier.calls)
	}
	if result.JudgeCommit == "" || h.installer.installs != 1 {
		t.Fatalf("result=%+v installs=%d", result, h.installer.installs)
	}
}

// Only the designer's own detector fires: the designer, not the deployer, gets
// the typed correction, the candidate is not accepted and no deployer attempt
// is spent.
func TestDesignerOwnedFiringCorrectsTheDesignerNotTheDeployer(t *testing.T) {
	h, fake := gateHarness(t, judgeFiring, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 2 {
		t.Fatalf("deployer attempts=%d judge runs=%d, want 1 and 2", h.workers.deployerAttempts(), h.workers.judgeRuns)
	}
	if fake.stages != 2 || fake.discards != 1 || fake.accepts != 1 {
		t.Fatalf("stages=%d discards=%d accepts=%d", fake.stages, fake.discards, fake.accepts)
	}
	previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
	if len(previous) == 0 || previous[0].GetRule() != "operability.O4.detectors" ||
		previous[0].GetPath() != ".sdo/diagnostics/detectors/health/frontend-available/manifest.yaml" ||
		!strings.Contains(previous[0].GetMessage(), "frontend has 0 ready replicas") {
		t.Fatalf("designer correction diagnostics = %v", previous)
	}
	if h.installer.installs != 1 {
		t.Fatalf("installs = %d", h.installer.installs)
	}
}

// A data check that fires on the candidate is the designer's file to correct:
// the correction names the check, not the detector Go installed for it.
func TestFiringDataCheckCorrectsTheDesignersCheckFile(t *testing.T) {
	firing := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: judgeFiring.Summary, Detectors: []lifecycle.DetectorState{{DetectorID: "data-orders", Status: "firing", Reason: "2 of 8 sampled records in order-db break the expected shape"}}}
	h, _ := gateHarness(t, firing, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
	if len(previous) == 0 || previous[0].GetPath() != ".sdo/diagnostics/data/orders.yaml" {
		t.Fatalf("designer correction diagnostics = %v", previous)
	}
}

// After DesignerAttempts corrections the same designer detectors still fire:
// the probe is probably right, so the firing is re-attributed once and the
// deployer gets it.
func TestUnchangedDesignerFiringAfterTheCapGoesToTheDeployer(t *testing.T) {
	h, fake := gateHarness(t, judgeFiring, judgeFiring, judgeFiring, clear)
	cfg := config()
	cfg.DesignerAttempts = 2
	if _, err := h.coordinator.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if h.workers.judgeRuns != 4 || h.workers.deployerAttempts() != 2 {
		t.Fatalf("judge runs=%d deployer attempts=%d, want 4 and 2", h.workers.judgeRuns, h.workers.deployerAttempts())
	}
	feedback := h.workers.deployer[1].GetCorrectionFeedback()
	if !strings.Contains(feedback, "source_fault") || !strings.Contains(feedback, "still firing after 2 designer corrections") {
		t.Fatalf("deployer feedback %q does not carry the re-attributed firing", feedback)
	}
	if fake.accepts != 1 || h.installer.installs != 1 {
		t.Fatalf("accepts=%d installs=%d", fake.accepts, h.installer.installs)
	}
}

// A detector set that changed between corrections is not "unchanged": the
// designer keeps the correction.
func TestChangedDesignerFiringStaysWithTheDesigner(t *testing.T) {
	other := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: judgeFiring.Summary, Detectors: []lifecycle.DetectorState{{DetectorID: "backend-available", Status: "firing", Reason: "backend down"}}}
	h, _ := gateHarness(t, judgeFiring, other, judgeFiring, clear)
	cfg := config()
	cfg.DesignerAttempts = 1
	if _, err := h.coordinator.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 4 {
		t.Fatalf("deployer attempts=%d judge runs=%d", h.workers.deployerAttempts(), h.workers.judgeRuns)
	}
}

// A detector Go wrote (the floor) firing is the application's fault whatever
// else fires: the deployer is corrected first and the designer is not asked.
func TestFloorFiringGoesToTheDeployerFirst(t *testing.T) {
	both := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationSourceFault, Summary: firing.Summary, Detectors: append(append([]lifecycle.DetectorState{}, judgeFiring.Detectors...), firing.Detectors...)}
	h, fake := gateHarness(t, both, clear)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 2 || !strings.Contains(h.workers.deployer[1].GetCorrectionFeedback(), "workload-health") {
		t.Fatalf("deployer requests = %v", h.workers.deployer)
	}
	if h.workers.judgeRuns != 2 || len(h.workers.judgeRequests[0].GetPreviousDiagnostics()) != 0 || len(h.workers.judgeRequests[1].GetPreviousDiagnostics()) != 0 {
		t.Fatalf("judge runs=%d: the designer must not be corrected for the application's fault", h.workers.judgeRuns)
	}
	if fake.discards != 1 || fake.accepts != 1 {
		t.Fatalf("discards=%d accepts=%d", fake.discards, fake.accepts)
	}
}

// A failing deployment check is cheap-first: the detector evaluation is not run
// and the cause routes by the table.
func TestLiveDeploymentFailureRoutesByCauseWithoutEvaluatingDetectors(t *testing.T) {
	t.Run("rollout goes to the deployer", func(t *testing.T) {
		h, _ := gateHarness(t, clear)
		h.readiness.live = [][]operability.Check{passLive(), failLive("O1.ready", operability.CauseRollout), passLive()}
		if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
			t.Fatal(err)
		}
		if h.workers.deployerAttempts() != 2 || !strings.Contains(h.workers.deployer[1].GetCorrectionFeedback(), "O1.ready [rollout]") {
			t.Fatalf("deployer requests = %v", h.workers.deployer)
		}
		if len(h.verifier.candidateRoots) != 1 {
			t.Fatalf("detectors evaluated %d times, want only for the passing candidate", len(h.verifier.candidateRoots))
		}
	})
	t.Run("an attribution defect ends the run", func(t *testing.T) {
		h, fake := gateHarness(t, clear)
		h.readiness.live = [][]operability.Check{passLive(), failLive("O2.stamps", operability.CauseAttribution)}
		_, err := h.coordinator.Run(context.Background(), config())
		var terminal *lifecycle.OperabilityError
		if !errors.As(err, &terminal) || terminal.Decision.Owner != operability.OwnerOperator {
			t.Fatalf("error = %v, want an operator-owned OperabilityError", err)
		}
		if len(h.verifier.candidateRoots) != 0 || fake.accepts != 0 || h.installer.installs != 0 || h.workers.deployerAttempts() != 1 {
			t.Fatalf("evaluations=%d accepts=%d installs=%d deployer attempts=%d", len(h.verifier.candidateRoots), fake.accepts, h.installer.installs, h.workers.deployerAttempts())
		}
	})
	t.Run("infrastructure is retried without spending an attempt", func(t *testing.T) {
		h, fake := gateHarness(t, clear)
		h.readiness.live = [][]operability.Check{passLive(), failLive("O1.deploy", operability.CauseInfrastructure), passLive()}
		if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
			t.Fatal(err)
		}
		if len(h.readiness.liveCalls) != 3 || h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 1 || fake.accepts != 1 {
			t.Fatalf("live calls=%d deployer attempts=%d judge runs=%d accepts=%d", len(h.readiness.liveCalls), h.workers.deployerAttempts(), h.workers.judgeRuns, fake.accepts)
		}
	})
}

// Accept refuses a candidate whose base moved: the candidate is staged again on
// the new head and verified again, within a bound.
func TestStaleCandidateIsRestagedAndReverified(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.staleAccepts = 1
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if fake.stages != 2 || fake.accepts != 2 || len(h.verifier.candidateRoots) != 2 || h.workers.judgeRuns != 1 || h.installer.installs != 1 {
		t.Fatalf("stages=%d accepts=%d verifications=%d judge runs=%d installs=%d", fake.stages, fake.accepts, len(h.verifier.candidateRoots), h.workers.judgeRuns, h.installer.installs)
	}
}

func TestPersistentlyStaleCandidateEndsTheRunWithoutInstalling(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.staleAccepts = 100
	_, err := h.coordinator.Run(context.Background(), config())
	if !errors.Is(err, broker.ErrCandidateStale) {
		t.Fatalf("error = %v, want ErrCandidateStale", err)
	}
	if h.installer.installs != 0 || fake.stages > 5 {
		t.Fatalf("installs=%d stages=%d", h.installer.installs, fake.stages)
	}
}

// A candidate the shared validator rejects at staging keeps the existing typed
// retry: the designer is re-dispatched with the diagnostics.
func TestStagingRejectionIsTheDesignersTypedRetry(t *testing.T) {
	h, fake := gateHarness(t, clear)
	fake.stageErrors = []error{&validate.Failure{Diagnostics: []validate.Diagnostic{{Rule: "memory.synthetic", Path: ".sdo/x", Message: "bad"}}}}
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
	if h.workers.judgeRuns != 2 || len(previous) != 1 || previous[0].GetRule() != "memory.synthetic" || len(h.verifier.candidateRoots) != 1 {
		t.Fatalf("judge runs=%d previous=%v verifications=%d", h.workers.judgeRuns, previous, len(h.verifier.candidateRoots))
	}
}

// A provided plan's application fault is terminal and typed with the report; the
// designer's own probe still gets its corrections first.
func TestProvidedPlanRoutesDetectorFaultToTheDesignerAndAppFaultToTheHuman(t *testing.T) {
	plan := providedPlans(t)["raw"]
	t.Run("application fault is terminal with the report", func(t *testing.T) {
		h, fake := gateHarness(t, firing)
		_, err := h.coordinator.Run(context.Background(), providedConfig(plan))
		var terminal *lifecycle.ProvidedPlanError
		if !errors.As(err, &terminal) || terminal.Stage != "verify" || terminal.Report == nil || terminal.Report.Stage != operability.StageComplete {
			t.Fatalf("error = %v, want a verify ProvidedPlanError with the complete-stage report", err)
		}
		if fake.accepts != 0 || h.installer.installs != 0 || h.workers.judgeRuns != 1 {
			t.Fatalf("accepts=%d installs=%d judge runs=%d", fake.accepts, h.installer.installs, h.workers.judgeRuns)
		}
	})
	t.Run("designer fault is corrected, then the capped firing is terminal", func(t *testing.T) {
		h, _ := gateHarness(t, judgeFiring)
		_, err := h.coordinator.Run(context.Background(), providedConfig(plan))
		var terminal *lifecycle.ProvidedPlanError
		if !errors.As(err, &terminal) || terminal.Stage != "verify" || h.workers.judgeRuns != 3 || h.workers.deployerAttempts() != 0 {
			t.Fatalf("error=%v judge runs=%d deployer attempts=%d, want 2 corrections then a terminal plan error", err, h.workers.judgeRuns, h.workers.deployerAttempts())
		}
	})
}

// A judge commit accepted by an earlier run (a crash before install) is
// verified, not staged again, and a failing verdict can no longer be corrected.
func TestResumedAcceptedJudgeCommitIsVerifiedBeforeInstall(t *testing.T) {
	for name, verdict := range map[string]lifecycle.HealthVerdict{"clear": clear, "firing": firing} {
		t.Run(name, func(t *testing.T) {
			h, fake := gateHarness(t, verdict)
			fake.accepted["op-judge-1"] = broker.Receipt{IncidentID: "op-judge-1", Phase: broker.InitialPhase, Commit: "commit-judge"}
			_, err := h.coordinator.Run(context.Background(), config())
			if fake.stages != 0 || fake.accepts != 0 || h.verifier.calls != 1 {
				t.Fatalf("stages=%d accepts=%d verifications=%d", fake.stages, fake.accepts, h.verifier.calls)
			}
			if (verdict.Outcome == lifecycle.VerificationClear) != (err == nil) || (err == nil) != (h.installer.installs == 1) {
				t.Fatalf("error=%v installs=%d", err, h.installer.installs)
			}
		})
	}
}

var _ = fmt.Sprint
var _ = contracts.MemoryDiagnostic{}

// An advance's designer refresh and the controller child's own start-up refresh
// are the same inputs on the same accepted commit. They must not share a
// publication: the proposal checkout, judge Job and rejection log of one would
// otherwise be attached to, or recreated under, the other when a child restarts
// during an advance.
func TestRefreshPublicationIsScopedToWhoRunsIt(t *testing.T) {
	revision := memory.HealthRevision{Commit: "commit-0", SourceInputs: "inputs", Objective: "stay up", DetectorRevision: "detectors", NeedsRefresh: true}
	run := func(scope string) string {
		h, fake := gateHarness(t, clear)
		fake.source = deployedSource()
		cfg := lifecycle.Config{Application: "app", Namespace: "ns", RefreshScope: scope, DesignerAttempts: 2, PendingEvaluations: 4, PendingInterval: time.Second, InfrastructureTries: 3, InfrastructureBackoff: time.Second}
		if _, err := h.coordinator.RefreshHealth(context.Background(), cfg, revision); err != nil {
			t.Fatalf("refresh %q: %v", scope, err)
		}
		if len(fake.candidates) != 1 {
			t.Fatalf("refresh %q staged %d candidates", scope, len(fake.candidates))
		}
		return fake.candidates[0].IncidentID
	}
	startup, advance, again := run(""), run(lifecycle.RefreshScopeAdvance), run(lifecycle.RefreshScopeAdvance)
	if startup == advance {
		t.Fatalf("start-up and advance refreshes share publication %q", startup)
	}
	if advance != again {
		t.Fatalf("an advance's refresh is not stable across restarts: %q then %q", advance, again)
	}
	if !strings.HasPrefix(advance, strings.SplitN(startup, "-base-", 2)[0]) {
		t.Fatalf("scoped id %q does not keep the input-bound prefix of %q", advance, startup)
	}
}

// The judge's probe never passes against the live application: the verdict is
// pending for the whole bounded window. The judge gets a correction turn that
// carries what the application actually answered, and no deployer attempt is
// spent.
func TestProbeThatNeverPassesCorrectsTheDesignerWithTheObservedResponse(t *testing.T) {
	const observed = `step open, GET /users/sign_in, status 200, HTTP 200 body lacks "Sign in to Example": observed title "Sign in · Example"`
	never := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationPending, Summary: "accepted health detectors report pending", Detectors: []lifecycle.DetectorState{{DetectorID: "traffic-probe-call", Status: "pending", Reason: "no synthetic scenario has succeeded since observation began: sign-in (last failure: " + observed + ")"}}}
	verdicts := []lifecycle.HealthVerdict{never, never, never, never, clear}
	h, fake := gateHarness(t, verdicts...)
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 2 {
		t.Fatalf("deployer attempts=%d judge runs=%d, want 1 and 2", h.workers.deployerAttempts(), h.workers.judgeRuns)
	}
	previous := h.workers.judgeRequests[1].GetPreviousDiagnostics()
	if len(previous) == 0 || previous[0].GetRule() != "operability.O4.detectors" ||
		previous[0].GetPath() != ".sdo/diagnostics/traffic/workloads/probe-call.yaml" ||
		!strings.Contains(previous[0].GetMessage(), "Sign in · Example") || !strings.Contains(previous[0].GetMessage(), "GET /users/sign_in") {
		t.Fatalf("correction diagnostics do not carry the observed response: %v", previous)
	}
	if fake.discards != 1 || fake.accepts != 1 || h.installer.installs != 1 {
		t.Fatalf("discards=%d accepts=%d installs=%d", fake.discards, fake.accepts, h.installer.installs)
	}
}

// A probe that keeps failing is not corrected forever: after the designer
// bound (DesignerAttempts corrections) the run ends with the pending window,
// naming what was observed, and no deployer attempt is spent.
func TestProbeThatNeverPassesEndsAfterTheDesignerBound(t *testing.T) {
	never := lifecycle.HealthVerdict{Outcome: lifecycle.VerificationPending, Summary: "accepted health detectors report pending", Detectors: []lifecycle.DetectorState{{DetectorID: "traffic-probe-call", Status: "pending", Reason: "no synthetic scenario has succeeded since observation began: sign-in"}}}
	h, _ := gateHarness(t, never)
	cfg := config()
	cfg.DesignerAttempts = 2
	_, err := h.coordinator.Run(context.Background(), cfg)
	var pending *lifecycle.PendingVerificationError
	if !errors.As(err, &pending) {
		t.Fatalf("error = %v, want PendingVerificationError", err)
	}
	if h.workers.deployerAttempts() != 1 || h.workers.judgeRuns != 3 || h.installer.installs != 0 {
		t.Fatalf("deployer attempts=%d judge runs=%d installs=%d", h.workers.deployerAttempts(), h.workers.judgeRuns, h.installer.installs)
	}
}
