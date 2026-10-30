package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The actual stage producer must feed fresh closure evidence into novelty
// admission, rather than its earlier bounded rollout observation.
func TestRecoverySettlementFeedsNoveltyAdmission(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "converged", true: "failed-repair"}[failed], func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			rollout := &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
			loop.Executor.Rollouts = rollout
			if err := RunResponseStages(context.Background(), state, []ResponseStage{&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}}); err != nil {
				t.Fatal(err)
			}
			before, err := responsePipelineResult(state, time.Now().Add(-time.Second), time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if len(before.PlanApplications) == 0 || before.PlanApplications[0].Result.VerificationPassed {
				t.Fatal("fixture did not produce unsettled applications")
			}
			if !failed {
				loop.Executor.Rollouts = observedDeploymentRollout{client: loop.Executor.Client.(TypedPlanClient).Client}
			}
			result, stages := loop.SettleRecoveryApplications(context.Background(), before, state.PriorStages)
			closure := IncidentClosure{Request: state.Request, Result: &result, VerifiedAt: time.Now(), FinalDetectorStates: []DetectorEvaluation{{DetectorID: loop.Detectors[0].Spec().ID, Status: DetectorEvaluationClear, EvaluatedAt: time.Now()}}, Frozen: true}
			closure.Request.PriorStages = stages
			derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
			if err != nil {
				t.Fatal(err)
			}
			if got := coveredRepeatCandidate(derived.Record); got == failed {
				t.Fatalf("novelty admission=%v failed=%v applications=%v", got, failed, derived.Record.PlanApplications)
			}
			for _, stage := range stages {
				for _, attempt := range stage.Mitigations {
					if !failed && attempt.Result != "applied_cleared" {
						t.Fatalf("stale attempt: %v", attempt)
					}
					if failed && attempt.ApplyReceipt.GetVerification().GetResult() == contracts.Verdict_RESULT_NO_MATCH {
						t.Fatal("failed rollout credited")
					}
				}
			}
		})
	}
}
