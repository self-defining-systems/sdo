package runtime

import (
	"context"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

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

