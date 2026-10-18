package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// skewedDispatcher returns a completed result whose responder-reported timing is
// offset from the controller's clock by an arbitrary, seed-chosen skew (the
// responder's node clock disagrees with the controller's). It records the
// dispatch request so the harness can read the incident id.
type skewedDispatcher struct {
	requests chan IncidentRequest
	skew     time.Duration
	calls    int
}

func (d *skewedDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.calls++
	d.requests <- request
	result := completedResult(request.IncidentID)
	skewed := time.Unix(0, 0).Add(d.skew).UTC()
	result.Timing = TimingMetrics{StartedAt: skewed, CompletedAt: skewed}
	return result, nil
}

// Dimension: clock skew between the responder and controller-observed times.
//
// The responder reports its start/complete timestamps from a node whose clock
// is skewed arbitrarily into the past or future relative to the controller. The
// invariant is that incident attribution timing in the closure is taken from
// the controller's own observed clock, never from the responder's report: the
// closure's DetectedAt/DispatchedAt/ResponderCompletedAt/VerifiedAt and
// HealthClearedAt stay monotonic and inside the controller's observed step
// window, so a skewed (even wildly future or negative) responder clock can
// neither pre-date a repair before dispatch nor push verification outside the
// window. The attribution booleans must also stay correct.
func TestChaosAttributionTimingIgnoresResponderClockSkew(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		rng := newChaosRNG(seed)
		// A skew anywhere in +/- ~100 days, including far-future and far-past.
		skew := time.Duration(rng.Int64N(int64(200*24*time.Hour))) - 100*24*time.Hour
		health := newChaosHealthDetector("health")
		dispatcher := &skewedDispatcher{requests: make(chan IncidentRequest, 8), skew: skew}
		controller, err := NewController(
			testControllerConfig(), []sdk.Detector{health},
			staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, chaosStart,
		)
		if err != nil {
			t.Fatalf("new controller: %v", err)
		}
		var now time.Time
		controller.now = func() time.Time { return now }
		jitter := newChaosRNG(seed ^ 0x5eed5eed5eed5eed)
		controller.dispatchJitter = jitter.Float64
		closed := make(chan IncidentClosure, 1)
		controller.OnIncidentClosed = func(c IncidentClosure) { closed <- c }

		health.active.set(true)
		dispatched := false
		recoverStep := 0

		var firstStep = time.Unix(1, 0).UTC()
		var lastStep time.Time
		for step := 0; step < chaosMaxSteps; step++ {
			now = time.Unix(int64(step+1), 0).UTC()
			lastStep = now
			if recoverStep > 0 && step >= recoverStep {
				health.active.set(false)
			}
			if err := controller.StepEvents(context.Background(), now, nil); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if !dispatched {
				if effect, ok := controller.PendingDispatchEffect(); ok {
					if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
						t.Fatalf("execute dispatch effect: %v", err)
					}
					awaitRequest(t, dispatcher.requests)
					awaitDispatchCompletionQueued(t, controller)
					dispatched = true
					recoverStep = step + 1 + rng.IntN(3)
				}
			}
			if dispatched && !controller.IncidentOpen() {
				break
			}
		}

		if dispatcher.calls != 1 {
			t.Fatalf("seed %d (skew %s): expected one dispatch, got %d", seed, skew, dispatcher.calls)
		}
		var closure IncidentClosure
		select {
		case closure = <-closed:
		case <-time.After(time.Second):
			t.Fatalf("seed %d (skew %s): incident never closed", seed, skew)
		}

		// Every attribution timestamp is controller-observed: inside the step
		// window and ordered, regardless of the responder's skewed report.
		inWindow := func(label string, ts time.Time) {
			if ts.Before(firstStep) || ts.After(lastStep) {
				t.Fatalf("seed %d (skew %s): closure %s=%s outside observed window [%s,%s] -- responder clock leaked",
					seed, skew, label, ts, firstStep, lastStep)
			}
		}
		inWindow("DetectedAt", closure.DetectedAt)
		inWindow("DispatchedAt", closure.DispatchedAt)
		inWindow("ResponderCompletedAt", closure.ResponderCompletedAt)
		inWindow("VerifiedAt", closure.VerifiedAt)
		if closure.HealthClearedAt != nil {
			inWindow("HealthClearedAt", *closure.HealthClearedAt)
		}
		if closure.DispatchedAt.Before(closure.DetectedAt) ||
			closure.ResponderCompletedAt.Before(closure.DispatchedAt) ||
			closure.VerifiedAt.Before(closure.ResponderCompletedAt) {
			t.Fatalf("seed %d (skew %s): closure timeline not monotonic: %+v", seed, skew, closure)
		}
	})
}
