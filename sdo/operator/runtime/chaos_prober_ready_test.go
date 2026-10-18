package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Dimension: fault injected before the prober is ready, and prober
// killed/restarted mid-incident.
//
// A detector whose backing signal is unavailable returns an error (an
// unreachable or not-yet-warm prober). The architecture requires that such an
// error blocks closure but never opens an incident. This dimension perturbs
// two windows: a seed-chosen "cold" window at the start where the fault is
// already present but the prober is unreachable (inject-before-ready), and a
// seed-chosen "kill" window mid-incident where the prober dies after the
// responder launched.
//
// Invariants: while the prober is unreachable before the incident, no incident
// opens and nothing is dispatched (the fault is not lost -- it is dispatched
// exactly once after the prober becomes ready); while the prober is unreachable
// during an open incident, the incident stays open (closure blocked, not
// prematurely closed) and the step loop never errors or spins; and once the
// prober returns and health clears, the incident closes exactly once.
func TestChaosUnreachableProberNeverOpensOrPrematurelyClosesAnIncident(t *testing.T) {
	const maxSteps = 40 // kept well under the verification timeout (60s at 1s/step)
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		rng := newChaosRNG(seed)
		health := newChaosHealthDetector("health")
		dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 8)}
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

		warmAt := rng.IntN(4) // prober becomes reachable at this step
		killLen := 1 + rng.IntN(3)

		dispatched := false
		dispatchStep := -1
		killStart, killEnd := -1, -1
		closed := false

		for step := 0; step < maxSteps; step++ {
			now = time.Unix(int64(step+1), 0).UTC()

			// The fault is present throughout until it is fixed after the prober
			// returns from the mid-incident kill window.
			erroring := step < warmAt
			active := true
			if dispatched {
				if step >= killStart && step < killEnd {
					erroring = true
				}
				if step >= killEnd {
					active = false // prober back and fault fixed -> health can clear
				}
			}
			health.erroring.set(erroring)
			health.active.set(active)

			if err := controller.StepEvents(context.Background(), now, nil); err != nil {
				t.Fatalf("seed %d step %d: step returned error (crash): %v", seed, step, err)
			}

			// Inject-before-ready: an unreachable prober must not open an incident.
			if step < warmAt {
				if controller.IncidentOpen() || dispatcher.callCount() != 0 {
					t.Fatalf("seed %d: incident opened while the prober was unreachable (step %d < warmAt %d)",
						seed, step, warmAt)
				}
			}

			if !dispatched {
				if effect, ok := controller.PendingDispatchEffect(); ok {
					if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
						t.Fatalf("execute dispatch effect: %v", err)
					}
					awaitRequest(t, dispatcher.requests)
					awaitDispatchCompletionQueued(t, controller)
					if err := controller.StepEvents(context.Background(), now, nil); err != nil {
						t.Fatalf("process completion: %v", err)
					}
					dispatched = true
					dispatchStep = step
					killStart = dispatchStep + 2
					killEnd = killStart + killLen
				}
			}

			// Prober killed mid-incident: closure stays blocked, incident open.
			if dispatched && step >= killStart && step < killEnd {
				if !controller.IncidentOpen() {
					t.Fatalf("seed %d: incident closed while the prober was unreachable mid-incident (step %d)", seed, step)
				}
			}

			if dispatched && step >= killEnd && !controller.IncidentOpen() {
				closed = true
				break
			}
		}

		if !dispatched {
			t.Fatalf("seed %d (warmAt=%d): fault never dispatched after the prober became ready", seed, warmAt)
		}
		if n := dispatcher.callCount(); n != 1 {
			t.Fatalf("seed %d: dispatched %d times, want exactly one", seed, n)
		}
		if !closed {
			t.Fatalf("seed %d: incident never closed after the prober returned and health cleared", seed)
		}
	})
}

var _ sdk.Detector = (*chaosDetector)(nil)
