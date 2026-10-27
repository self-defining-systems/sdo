package runtime

import (
	"context"
	"testing"
	"time"
)

// An exhausted stage budget hands every match to the agent with a reason and
// writes nothing.
func TestDeterministicResponseStageBudgetExhaustedHandsMatchesToAgent(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: time.Nanosecond}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 3 || len(loop.PlanApplications()) != 0 || len(state.Findings) < 3 {
		t.Fatalf("budget exhaustion must apply nothing: %v, findings=%d", report, len(state.Findings))
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "skipped" || attempt.SkipReason != "stage_budget" {
			t.Fatalf("unapplied match must carry its reason: %v", attempt)
		}
	}
}

// The one rollout wait ends within the stage budget, however many bindings
// share the apply.
func TestDeterministicResponseStageRolloutWaitIsBoundedByBudget(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	rollout := &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
	loop.Executor.Rollouts = rollout
	const budget = 20 * time.Second
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: budget}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if rollout.observed <= 0 || rollout.observed > budget || len(loop.PlanApplications()) != 1 {
		t.Fatalf("rollout wait %s outside the %s budget, applications=%d", rollout.observed, budget, len(loop.PlanApplications()))
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "rollout_pending_at_handoff" {
			t.Fatalf("pending rollout must reach the agent: %v", attempt)
		}
	}
}

