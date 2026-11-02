//go:build integration

package runtime

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

// Observation exhaustion cannot suppress accepted covered repairs.
func TestDeterministicResponseStageBudgetExhaustedAppliesEveryBinding(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: time.Nanosecond}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Mitigations) != 3 || len(loop.PlanApplications()) != 3 || len(state.Findings) != 0 {
		t.Fatalf("budget exhaustion must still apply every binding: %v, findings=%d", report, len(state.Findings))
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "applied_cleared" || attempt.SkipReason != "" {
			t.Fatalf("covered match must have a completed repair: %v", attempt)
		}
	}
}

// Every rollout wait ends within the stage budget. A pending handoff keeps the
// pass, so each binding still gets its own apply under what remains.
func TestDeterministicResponseStageRolloutWaitIsBoundedByBudget(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, false, "api2")
	rollout := &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
	loop.Executor.Rollouts = rollout
	const budget = 20 * time.Second
	report, err := (&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: budget}).Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	if rollout.observed <= 0 || rollout.observed > budget || len(loop.PlanApplications()) != len(report.Mitigations) || len(report.Mitigations) != 3 {
		t.Fatalf("rollout wait %s outside the %s budget, applications=%d", rollout.observed, budget, len(loop.PlanApplications()))
	}
	for _, attempt := range report.Mitigations {
		if attempt.Result != "rollout_pending_at_handoff" {
			t.Fatalf("pending rollout must reach the agent: %v", attempt)
		}
	}
}

// A rollout deliberately remains pending while independent covered bindings
// are repaired. The blocking interface models old serial rollout polling;
// production must use its one-sample interface before opening the worker.
func TestStageMultiBindingScriptedSessionStartup(t *testing.T) {
	for _, budget := range []time.Duration{time.Nanosecond, time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			loop, state := pipelineTestLoopWithServices(t, false, "api2", "api3", "api4", "api5")
			rollout := &startupStageRollout{base: loop.Executor.Rollouts}
			loop.Executor.Rollouts = rollout
			loop.Detectors = append(loop.Detectors, startupResidualDetector{})
			session := &startupSessionDispatcher{detected: time.Now()}
			if err := RunResponseStages(context.Background(), state, []ResponseStage{
				&DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, Budget: budget},
				AgentResponseStage{Dispatcher: session},
			}); err != nil {
				t.Fatal(err)
			}
			t.Logf("scripted detection_to_session_open=%s bindings=%d blocking_rollout_calls=%d", session.opened.Sub(session.detected), len(loop.PlanApplications()), rollout.waits)
			if session.opened.IsZero() || len(loop.PlanApplications()) != 6 || len(session.request.Findings) != 1 || session.request.Findings[0].PrimaryResource.Kind != "Deployment" {
				t.Fatalf("startup lost covered repairs or remaining work: opened=%v applications=%d findings=%v", session.opened, len(loop.PlanApplications()), session.request.Findings)
			}
			if rollout.waits != 0 {
				t.Fatalf("independent bindings serialized on %d blocking rollout calls", rollout.waits)
			}
			for _, attempt := range state.PriorStages[0].Mitigations {
				if !attempt.GetApplyReceipt().GetApplied() || attempt.SkipReason != "" {
					t.Fatalf("covered binding was not written: %v", attempt)
				}
			}
		})
	}
}

type startupStageRollout struct {
	base  PlanRollouts
	waits int
}

func (r *startupStageRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	r.waits++
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return r.Observe(ctx, refs)
}

func (r *startupStageRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	for _, ref := range refs {
		if ref.Kind == "Deployment" {
			return context.DeadlineExceeded
		}
	}
	return r.base.Wait(ctx, refs)
}

type startupSessionDispatcher struct {
	detected, opened time.Time
	request          IncidentRequest
}

func (d *startupSessionDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.opened, d.request = time.Now(), request
	result := completedResult(request.IncidentID)
	result.ResponderSessionID = "synthetic-stage-startup-session"
	return result, nil
}

// Expired caller and incident deadlines must never reach a rollout consumer,
// whether it supports polling or only the blocking interface.
func TestRolloutBudgetNeverInvokesConsumerAfterDeadline(t *testing.T) {
	for _, poll := range []bool{false, true} {
		for _, supportsObserve := range []bool{false, true} {
			for _, expiredIncident := range []bool{false, true} {
				t.Run(fmt.Sprintf("poll=%t/observe=%t/incident=%t", poll, supportsObserve, expiredIncident), func(t *testing.T) {
					rollout := &pipelineDeadlineRollout{failure: context.DeadlineExceeded}
					executor := PlanExecutor{Rollouts: rollout, PollRollouts: poll}
					if supportsObserve {
						executor.Rollouts = pipelineDeadlineObserver{rollout}
					}
					ctx := context.Background()
					request := IncidentRequest{}
					if expiredIncident {
						request.ResponseDeadline = time.Now().Add(-time.Second)
					} else {
						var cancel context.CancelFunc
						ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
						defer cancel()
					}
					pending, err := executor.waitForRollouts(ctx, request, []sdk.ObjectRef{{Kind: "Deployment", Name: "api", Namespace: "app"}})
					if pending == nil || err == nil || rollout.calls != 0 {
						t.Fatalf("expired budget reached consumer or lost pending proof: calls=%d pending=%v err=%v", rollout.calls, pending, err)
					}
				})
			}
		}
	}
}

type pipelineDeadlineObserver struct{ *pipelineDeadlineRollout }

func (r pipelineDeadlineObserver) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Wait(ctx, refs)
}

