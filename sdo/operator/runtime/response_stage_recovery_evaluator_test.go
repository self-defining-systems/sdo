package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Recovered needs every alert-set detector evaluated clear. A detector that
// clears once and then stops answering has not shown that the alert stayed
// clear: its silence never earns the stage credit, exactly as closure would not
// call it Recovered, and the responder gets the incident.
func TestStageNeverCreditsAnAlertDetectorThatStoppedAnswering(t *testing.T) {
	stage, state, clock, _ := settlingStage(t, settlingCase{healAfter: 20 * time.Second, stallWindow: 60 * time.Second, stability: 40 * time.Second, stopsAnswering: true, responseDeadline: 10 * time.Minute})
	started := clock.Now()
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	report := state.PriorStages[0]
	if got := report.GetRecoveryWait().GetExitReason(); got == contracts.RecoveryWait_EXIT_REASON_CLEARED || !agent.dispatched {
		t.Fatalf("exit = %s after %s, dispatched = %v: an unavailable detector is not clear", got, clock.Now().Sub(started), agent.dispatched)
	}
	if waited := clock.Now().Sub(started); waited > 5*time.Minute {
		t.Fatalf("the stage observed for %s; its own deadline, not the response deadline, ends it", waited)
	}
}

// The hand-off deadline is fixed once. A written workload that keeps becoming
// Ready and observations that each take long never buy the stage more time:
// its recovery observation stops at the deadline it persisted, and nothing
// extends it afterwards.
func TestStageObservesNoLaterThanItsPersistedDeadline(t *testing.T) {
	tc := settlingCase{probe: true, stallWindow: 60 * time.Second, readyEvery: 15 * time.Second, slowObserve: 20 * time.Second, stability: 40 * time.Second}
	stage, state, clock, _ := settlingStage(t, tc)
	stage.Budget = 30 * time.Second
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	loop := stage.NewLoop(state.Request)
	deadline := loop.stageProgress[stage.Name()].RecoveryDeadline
	if deadline.IsZero() {
		t.Fatal("the stage persisted no deadline")
	}
	// The wait's own observation stops at the deadline; only the stage's
	// closing bookkeeping (one snapshot for equivalence crediting) follows it.
	if over := clock.Now().Sub(deadline); over > tc.slowObserve {
		t.Fatalf("the stage observed %s past its persisted deadline", over)
	}
	if !agent.dispatched {
		t.Fatal("the probe still fails and nobody acted")
	}
}

// The configured wait is the wait: a learned repair whose rollout takes four
// of the five configured minutes is credited, whatever share of the response
// budget that leaves; only the response deadline bounds it.
func TestAfterPlaybookWaitKeepsItsConfiguredBound(t *testing.T) {
	stage, state, clock, _ := settlingStage(t, settlingCase{healAfter: 4 * time.Minute, stallWindow: 60 * time.Second, stability: 40 * time.Second, responseDeadline: 6 * time.Minute})
	stage.WaitBound = 5 * time.Minute
	started := clock.Now()
	agent := &recordingStage{}
	if err := RunResponseStages(context.Background(), withPolicy(state, settings.AfterPlaybookWait), []ResponseStage{stage, agent}); err != nil {
		t.Fatal(err)
	}
	if got := state.PriorStages[0].GetRecoveryWait().GetExitReason(); got != contracts.RecoveryWait_EXIT_REASON_CLEARED || agent.dispatched {
		t.Fatalf("exit = %s after %s, dispatched = %v; want CLEARED inside the 5 min wait", got, clock.Now().Sub(started), agent.dispatched)
	}
}

// The claim-time check judges the incident's alert set, not the residual
// findings the hand-off carried. An opening probe whose finding is no longer
// in the request cannot be evaluated, so it is not recovered: the claim never
// credits it, and the worker runs.
func TestRecoveredClaimNeverCreditsAnOpeningProbeItCannotEvaluate(t *testing.T) {
	probe := sdk.Finding{DetectorID: "traffic-checkout", RuleID: "scenario-slo.buy", Status: sdk.FindingActive}
	request := IncidentRequest{IncidentID: "synthetic", OpeningKeys: []string{findingKey(probe)},
		PriorStages: []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{Result: "applied_still_matching", ApplyReceipt: &contracts.PlanApplication{Applied: true}}}}}}
	detectors := []sdk.Detector{controllerDetector("traffic-checkout", time.Second, sdk.Finding{})}
	loop := NewResponderLoop(request, nil, staticProvider{}, detectors, PlanExecutor{}, 2)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	loop.stageProgress = map[string]responseStageProgress{}
	state, err := (KubernetesJobDispatcher{}).beforeFirstResponderClaim(context.Background(), request, &contracts.AgentTask{}, loop)
	if err != nil {
		t.Fatal(err)
	}
	if state != nil && deterministicStageReport(state.PriorStages).GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatal("the claim credited an opening probe it could not evaluate")
	}
}

// recovered fails closed on an alert-set user test it cannot evaluate.
func TestRecoveredHoldsAnAlertSetProbeWithoutAFinding(t *testing.T) {
	probe := sdk.Finding{DetectorID: "traffic-checkout", RuleID: "scenario-slo.buy", Status: sdk.FindingActive}
	check := recovered(verificationInput{view: sdktest.Snapshot{}, request: IncidentRequest{OpeningKeys: []string{findingKey(probe)}}})
	if check.verdict != recoveryPending {
		t.Fatalf("verdict = %v, want pending: %s", check.verdict, check.reason)
	}
}
