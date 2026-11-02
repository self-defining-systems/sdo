//go:build integration

package runtime

import (
	"context"
	"errors"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func TestStageRetainsTypedPendingFromRolloutProducer(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	now := time.Now()
	start := now
	loop.Executor.Rollouts = stage3TypedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: start.Add(30 * time.Second)}
	stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, recoveryWait: func(context.Context, time.Duration) error { now = now.Add(15 * time.Second); return nil }}
	dispatcher := &startupSessionDispatcher{}
	if err := RunResponseStages(context.Background(), state, []ResponseStage{stage, AgentResponseStage{Dispatcher: dispatcher}}); err != nil {
		t.Fatal(err)
	}
	if !dispatcher.opened.IsZero() || now.Sub(start) != 30*time.Second {
		t.Fatalf("typed pending became failure: opened=%v wait=%s reports=%v", dispatcher.opened, now.Sub(start), state.PriorStages)
	}
}

type stage3TypedRollout struct {
	base  PlanRollouts
	now   func() time.Time
	ready time.Time
}

func (r stage3TypedRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.Observe(ctx, refs)
}
func (r stage3TypedRollout) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.now().Before(r.ready) {
		return &RolloutPendingError{Pending: &contracts.RolloutPending{Reason: "synthetic producer is still observing rollout"}}
	}
	return r.base.Wait(ctx, refs)
}

func assertStage3Wait(t *testing.T, loop *ResponderLoop, report *contracts.StageReport, reason contracts.RecoveryWait_ExitReason, duration time.Duration) {
	t.Helper()
	if report.GetRecoveryWait() == nil || report.GetRecoveryWait().GetExitReason() != reason || report.GetRecoveryWait().GetDuration().AsDuration() != duration {
		t.Fatalf("wait telemetry: got %v want reason=%s duration=%s", report.GetRecoveryWait(), reason, duration)
	}
	for _, attempt := range report.Mitigations {
		if attempt.ApplyReceipt != nil && !proto.Equal(attempt.ApplyReceipt.GetRecoveryWait(), report.RecoveryWait) {
			t.Fatalf("stage receipt lost wait: %v", attempt)
		}
	}
	for _, application := range loop.ApplicationEvidence() {
		encoded, err := recordedPlanContract(application)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(encoded.GetRecoveryWait(), report.RecoveryWait) {
			t.Fatalf("application receipt lost wait: %v", encoded)
		}
		restored := planApplicationState(encoded)
		again, err := recordedPlanContract(restored)
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(again.GetRecoveryWait(), report.RecoveryWait) {
			t.Fatal("restore lost wait")
		}
	}
	if err := wire.Validate(report); err != nil {
		t.Fatal(err)
	}
}

func TestStageWaitExitReasons(t *testing.T) {
	for _, mode := range []string{"cleared", "deadline", "blocker", "failed"} {
		t.Run(mode, func(t *testing.T) {
			loop, state := pipelineTestLoop(t, false)
			now := time.Now()
			start := now
			want := contracts.RecoveryWait_EXIT_REASON_CLEARED
			switch mode {
			case "deadline":
				want = contracts.RecoveryWait_EXIT_REASON_DEADLINE
				loop.Executor.Rollouts = &settle2DelayedRollout{base: loop.Executor.Rollouts, now: func() time.Time { return now }, ready: start.Add(5 * time.Minute)}
			case "blocker":
				want = contracts.RecoveryWait_EXIT_REASON_BLOCKER
				loop.Executor.Rollouts = settle2BlockedRollout{}
			case "failed":
				want = contracts.RecoveryWait_EXIT_REASON_FAILED
				loop.Executor.Rollouts = &pipelineDeadlineRollout{failure: errors.New("synthetic rollout failed")}
			}
			stage := &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }, recoveryNow: func() time.Time { return now }, ObservationCeiling: time.Second, recoveryWait: func(_ context.Context, d time.Duration) error { now = now.Add(d); return nil }}
			report, err := stage.Run(context.Background(), state)
			if err != nil {
				t.Fatal(err)
			}
			assertStage3Wait(t, loop, report, want, now.Sub(start))
			if mode == "deadline" && now.Sub(start) != time.Second {
				t.Fatalf("deadline wait=%s", now.Sub(start))
			}
		})
	}
}
