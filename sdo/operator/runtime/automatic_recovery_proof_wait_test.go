package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// clearingUserTest is user test buy's alert: active until cleared, as its
// detector reports once the repair reaches users.
type clearingUserTest struct{ cleared atomic.Bool }

func (*clearingUserTest) Spec() sdk.DetectorSpec { return (&stuckUserTest{}).Spec() }

func (d *clearingUserTest) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.cleared.Load() {
		return []sdk.Finding{}, nil
	}
	return (&stuckUserTest{}).Detect(ctx, view)
}

// The stage's write landed and every alert cleared, but a joined user test
// that samples every three minutes has too few samples since the settle for
// Recovered to hold at the stage's bound. Clear alerts leave nothing to hand
// off and nothing failed: the stage waits for Recovered's own horizon and the
// incident closes as the stage's success, never escalated.
func TestClearAlertsWaitForRecoveredInsteadOfEscalating(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookAgent, settings.AfterPlaybookWait} {
		t.Run(policy, func(t *testing.T) {
			detector := &clearingUserTest{}
			c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 5 * time.Second}, policy, detector)
			test := &buyTest{every: 3 * time.Minute, failing: func(time.Duration) bool { return false }, start: world.clock.Now()}
			world.probes = test.window
			found, _ := detector.Detect(t.Context(), nil)
			if !c.joinAlertSet(found[0], test.start) {
				t.Fatal("the user test did not join")
			}
			report := autoReport(c)
			bound := report.GetRecoveryDeadline().AsTime()
			stepSeconds(t, c, world, 20)
			detector.cleared.Store(true)
			if horizon := test.window(test.start)["shop"].Workload.ScenarioHorizon("buy"); !bound.Before(test.start.Add(horizon)) {
				t.Fatalf("the case needs a stage bound %s before the test's horizon %s", bound.Sub(test.start), horizon)
			}
			for i := 0; c.IncidentOpen() && i < 15*60; i++ {
				autoStep(t, c, world.clock, time.Second)
				if !c.escalatedAt.IsZero() || report.GetHandoffSelected() {
					t.Fatalf("clear alerts at %s: escalated=%v handoff=%v, waiting on %q",
						world.clock.Now().Sub(test.start), c.escalatedAt, report.GetHandoffSelected(), c.recoveryWaitingOn)
				}
			}
			if c.IncidentOpen() || closed.Result == nil || !closed.EscalatedAt.IsZero() {
				t.Fatalf("Recovered did not close the incident: open=%v escalated=%v", c.IncidentOpen(), closed.EscalatedAt)
			}
			stage := deterministicStageReport(closed.Request.PriorStages)
			if exit := stage.GetRecoveryWait().GetExitReason(); exit != contracts.RecoveryWait_EXIT_REASON_CLEARED {
				t.Fatalf("stage exit = %s, want cleared", exit)
			}
			if closed.VerifiedAt.Before(bound) {
				t.Fatalf("verified at %s, before the stage bound %s: the case does not exercise the wait", closed.VerifiedAt, bound)
			}
			derived, err := (ProtoOutcomeDeriver{}).Derive(t.Context(), *cloneIncidentClosure(closed), "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if got := derived.Record.GetClassification(); got != "success" {
				t.Fatalf("classification = %q, want success", got)
			}
			for _, app := range derived.Record.GetPlanApplications() {
				if reason := app.GetSettlementRejectionReason(); reason != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED {
					t.Fatalf("the stage's write was rejected at settlement: %s", reason)
				}
			}
		})
	}
}

// A user test that never reaches its sample count does not hold a stage with
// clear alerts forever: past Recovered's horizon the stage escalates as before.
func TestClearAlertsEscalatePastRecoveredsHorizon(t *testing.T) {
	detector := &clearingUserTest{}
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 5 * time.Second}, settings.AfterPlaybookAgent, detector)
	test := &buyTest{every: 2 * time.Minute, failing: func(time.Duration) bool { return false }, start: world.clock.Now()}
	test.silent = func(at time.Duration) bool { return at > 0 }
	world.probes = test.window
	found, _ := detector.Detect(t.Context(), nil)
	if !c.joinAlertSet(found[0], test.start) {
		t.Fatal("the user test did not join")
	}
	stepSeconds(t, c, world, 20)
	detector.cleared.Store(true)
	horizon := test.window(test.start)["shop"].Workload.ScenarioHorizon("buy")
	for i := 0; c.escalatedAt.IsZero() && i < 15*60; i++ {
		autoStep(t, c, world.clock, time.Second)
	}
	if c.escalatedAt.IsZero() || c.escalatedAt.Before(test.start.Add(horizon)) || c.escalatedAt.After(test.start.Add(horizon+time.Minute)) {
		t.Fatalf("escalated at %s, want just past the horizon %s (closed %+v)", c.escalatedAt.Sub(test.start), horizon, closed.EscalatedAt)
	}
}
