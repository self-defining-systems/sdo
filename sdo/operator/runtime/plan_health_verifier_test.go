package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type settlingPlanHealth struct {
	calls  atomic.Int32
	always bool
}

func (*settlingPlanHealth) Spec() sdk.DetectorSpec {
	spec := (planTestDetector{}).Spec()
	spec.ID = "settling-health"
	return spec
}

func (d *settlingPlanHealth) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.calls.Add(1) == 1 || d.always {
		return []sdk.Finding{stateFinding("settling-health")}, nil
	}
	return nil, nil
}

func TestPlanHealthVerifierWaitsForFreshHealthyObservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	detector := &settlingPlanHealth{}
	verifier := PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, Timeout: time.Second, PollInterval: time.Millisecond}
	passed, err := verifier.Verify(ctx)
	if err != nil || !passed || detector.calls.Load() < 2 {
		t.Fatalf("stale post-write health ended verification: passed=%t calls=%d err=%v", passed, detector.calls.Load(), err)
	}
}

func TestPlanHealthVerifierStopsAtItsBudget(t *testing.T) {
	detector := &settlingPlanHealth{always: true}
	verifier := PlanHealthVerifier{Detectors: []sdk.Detector{detector}, Provider: &changingProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, Timeout: 5 * time.Millisecond, PollInterval: time.Millisecond}
	passed, err := verifier.Verify(context.Background())
	if err != nil || passed || detector.calls.Load() == 0 {
		t.Fatalf("unhealthy verification escaped its budget: passed=%t calls=%d err=%v", passed, detector.calls.Load(), err)
	}
}
