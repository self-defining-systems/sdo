package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// TRIAL1 hotel n2: an unsuccessful workload repair preceded the already
// matched quota repair. Every independent match must still get its turn,
// regardless of where an earlier application fails.
func TestStageApplicationFailureDoesNotStarveOtherMatches(t *testing.T) {
	for _, failureIndex := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(failureIndex), func(t *testing.T) {
			loop, state := pipelineTestLoopWithServices(t, false, "api2")
			rollout := &stageIndexedRollout{base: loop.Executor.Rollouts, failureIndex: failureIndex}
			loop.Executor.Rollouts = rollout
			report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Mitigations) != 3 || len(loop.PlanApplications()) != 3 {
				t.Fatalf("one failed application starved independent matches: %v", report)
			}
			for i, attempt := range report.Mitigations {
				want := "applied_cleared"
				if i == failureIndex {
					want = "rollout_failed"
				}
				if attempt.Result != want {
					t.Fatalf("attempt %d: want %s, got %v", i, want, attempt)
				}
			}
		})
	}
}

type stageIndexedRollout struct {
	base         PlanRollouts
	failureIndex int
	calls        int
}

func (r *stageIndexedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	index := r.calls
	r.calls++
	if index == r.failureIndex {
		return errors.New("synthetic rollout failure")
	}
	return r.base.Wait(ctx, refs)
}

// Inspect real executor deadlines rather than sleep for the live 45-second
// budget. A first rollout must reserve time for every still-unattempted match.
func TestStageRolloutReservesBudgetForRemainingMatches(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2", "api3")
	rollout := &stageBudgetRollout{base: loop.Executor.Rollouts}
	loop.Executor.Rollouts = rollout
	const budget = 40 * time.Second
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: budget}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(rollout.budgets) != 4 || len(report.Mitigations) != 4 {
		t.Fatalf("not every match reached the executor: %v", report)
	}
	for index, got := range rollout.budgets {
		if got <= 0 || got > budget/time.Duration(4-index) {
			t.Errorf("attempt %d monopolizes the stage: %s", index, got)
		}
	}
}

type stageBudgetRollout struct {
	base    PlanRollouts
	budgets []time.Duration
}

func (r *stageBudgetRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		return errors.New("missing rollout deadline")
	}
	r.budgets = append(r.budgets, time.Until(deadline))
	return r.base.Wait(ctx, refs)
}

func TestStageFailedWriteKeepsItsFindingAndAppliesIndependentMatches(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	loop.Executor.Client = stageFailedWriteClient{PlanObjectClient: loop.Executor.Client}
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 3 || len(state.Findings) != 1 || state.Findings[0].RuleID != "image-pull" {
		t.Fatalf("failed write hid its finding or starved independent repairs: %v findings=%v", report, state.Findings)
	}
	for i, attempt := range report.Mitigations {
		if i == 0 {
			if attempt.Result != "apply_error" || attempt.GetApplyReceipt().GetApplied() {
				t.Fatalf("failed write claimed recovery: %v", attempt)
			}
		} else if attempt.Result != "applied_cleared" {
			t.Fatalf("independent match not applied: %v", attempt)
		}
	}
}

type stageFailedWriteClient struct{ PlanObjectClient }

func (c stageFailedWriteClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, data []byte) error {
	if ref.Kind == "Deployment" {
		return errors.New("synthetic Deployment write refused")
	}
	return c.PlanObjectClient.Patch(ctx, ref, kind, data)
}
