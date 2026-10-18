package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Dimension: responder dispatch delayed / responder slow.
//
// A responder is held in flight for a seed-chosen number of steps while a
// second incident-class finding activates at a seed-chosen step (which may land
// before, during, or after dispatch). The invariants are that a single fault
// episode produces exactly one dispatch and exactly one closure -- the incident
// lock holds across the whole overlap, so no second responder is launched and
// no duplicate incident is opened -- and that the controller never spins
// (bounded steps) while the responder is in flight.
func TestChaosResponderOverlapNeverDoubleDispatches(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		rng := newChaosRNG(seed)
		health := newChaosHealthDetector("health")
		extra := newChaosIncidentDetector("extra")
		dispatcher := &blockingDispatcher{
			requests: make(chan IncidentRequest, 8),
			release:  make(chan struct{}),
		}
		controller, err := NewController(
			testControllerConfig(), []sdk.Detector{health, extra},
			staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, chaosStart,
		)
		if err != nil {
			t.Fatalf("new controller: %v", err)
		}
		var now time.Time
		controller.now = func() time.Time { return now }
		jitter := newChaosRNG(seed ^ 0x5eed5eed5eed5eed)
		controller.dispatchJitter = jitter.Float64
		closes := 0
		controller.OnIncidentClosed = func(IncidentClosure) { closes++ }

		injectAt := 1 + rng.IntN(2)
		extraAt := rng.IntN(8)

		requests := 0
		dispatched := false
		released := false
		var incidentID string
		releaseAt := -1
		recoverAt := -1

		for step := 0; step < chaosMaxSteps; step++ {
			now = time.Unix(int64(step+1), 0).UTC()
			if step == injectAt {
				health.active.set(true)
			}
			if step == extraAt {
				extra.active.set(true)
			}
			if recoverAt >= 0 && step >= recoverAt {
				health.active.set(false)
				extra.active.set(false)
			}
			if err := controller.StepEvents(context.Background(), now, nil); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}

			switch {
			case !dispatched:
				if effect, ok := controller.PendingDispatchEffect(); ok {
					if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
						t.Fatalf("execute dispatch effect: %v", err)
					}
					incidentID = awaitRequest(t, dispatcher.requests).IncidentID
					requests++
					dispatched = true
					releaseAt = step + 1 + rng.IntN(4)
				}
			case !released:
				// The incident lock must hold: no second effect, no second request.
				if _, ok := controller.PendingDispatchEffect(); ok {
					t.Fatalf("seed %d: a second dispatch effect appeared while the responder was in flight", seed)
				}
				if drainExtraRequest(dispatcher.requests) {
					t.Fatalf("seed %d: a second responder was dispatched while one was in flight", seed)
				}
				if step >= releaseAt {
					close(dispatcher.release)
					released = true
					awaitDispatchCompletionQueued(t, controller)
					recoverAt = step + 1 + rng.IntN(3)
				}
			default:
				if !controller.IncidentOpen() {
					break
				}
			}

			if dispatched && released && !controller.IncidentOpen() {
				break
			}
		}

		if !released && dispatched {
			close(dispatcher.release) // avoid leaking the blocked dispatch goroutine
		}
		if !dispatched {
			t.Fatalf("seed %d: fault never dispatched", seed)
		}
		if requests != 1 {
			t.Fatalf("seed %d: expected exactly one responder dispatch, got %d", seed, requests)
		}
		if controller.IncidentOpen() {
			t.Fatalf("seed %d: incident never closed after recovery (incident %s)", seed, incidentID)
		}
		if closes != 1 {
			t.Fatalf("seed %d: incident closed %d times, want exactly one", seed, closes)
		}
	})
}

// drainExtraRequest reports whether a second dispatch request is already queued,
// without blocking.
func drainExtraRequest(requests <-chan IncidentRequest) bool {
	select {
	case <-requests:
		return true
	default:
		return false
	}
}
