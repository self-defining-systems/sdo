package runtime

import (
	"context"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Dimension: a fault injected early vs late relative to a controller
// crash/restart (controller-resume), with the restart landing at a seed-chosen
// point relative to the fault firing and the responder launch.
//
// The controller is discarded and rebuilt on the same durable state and shares
// one dispatcher across the restart. Whether the fault is injected before or
// after the crash, and whether the pending dispatch was executed before the
// crash or only recovered after it, the invariants are: the incident is never
// lost (it dispatches and closes), it is dispatched exactly once across the
// whole run (a restart neither re-dispatches an already-launched incident nor
// drops a persisted-but-unlaunched one), and the incident id is stable across
// the restart (the recovered effect keeps its durable id).
func TestChaosRestartDispatchesExactlyOnceWithStableIncidentID(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		ctx := context.Background()
		rng := newChaosRNG(seed)
		jitter := newChaosRNG(seed ^ 0x5eed5eed5eed5eed)

		detector := newChaosHealthDetector("health") // shared across the restart
		dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 8)}
		client := fake.NewSimpleClientset()
		store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")

		var now time.Time
		build := func() *Controller {
			c, err := NewController(
				testControllerConfig(), []sdk.Detector{detector},
				staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, chaosStart,
			)
			if err != nil {
				t.Fatalf("new controller: %v", err)
			}
			c.now = func() time.Time { return now }
			c.dispatchJitter = jitter.Float64
			if err := c.AttachStateStore(ctx, store); err != nil {
				t.Fatalf("attach store: %v", err)
			}
			return c
		}

		injectStep := rng.IntN(5)
		crashStep := 2 + rng.IntN(8)
		executeBeforeCrash := rng.IntN(2) == 0

		c := build()
		wantID := ""
		dispatched := false
		closed := false
		recoverStep := -1

		for step := 0; step < chaosMaxSteps; step++ {
			now = time.Unix(int64(step+1), 0).UTC()
			if step == injectStep {
				detector.active.set(true)
			}
			if recoverStep >= 0 && step >= recoverStep {
				detector.active.set(false)
			}
			if step == crashStep {
				if err := c.PersistState(ctx); err != nil {
					t.Fatalf("persist before crash: %v", err)
				}
				c = build() // the controller crashes and relaunches on durable state
			}

			if err := c.StepEvents(ctx, now, nil); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if err := c.PersistState(ctx); err != nil {
				t.Fatalf("persist at step %d: %v", step, err)
			}

			effect, pending := c.PendingDispatchEffect()
			if pending && wantID == "" {
				wantID = effect.Request.IncidentID
			}
			if !dispatched && pending {
				deferForCrash := !executeBeforeCrash && step < crashStep
				if !deferForCrash {
					if err := c.ExecuteDispatchEffect(ctx, effect); err != nil {
						t.Fatalf("execute dispatch effect: %v", err)
					}
					request := awaitRequest(t, dispatcher.requests)
					awaitDispatchCompletionQueued(t, c)
					if request.IncidentID != wantID {
						t.Fatalf("seed %d: dispatched id %q != durable id %q (id not stable across restart)",
							seed, request.IncidentID, wantID)
					}
					// Process the completion and persist it now, so responderDone is
					// durable before any later crash. (The real idempotent Job
					// dispatcher reattaches to its Job across a crash in this narrow
					// window; recordingDispatcher cannot, and that reattach path is
					// covered by TestKubernetesJobDispatcherCrashReplayReattachesExactlyOnce.)
					if err := c.StepEvents(ctx, now, nil); err != nil {
						t.Fatalf("process completion: %v", err)
					}
					if err := c.PersistState(ctx); err != nil {
						t.Fatalf("persist completion: %v", err)
					}
					dispatched = true
					recoverStep = step + 1 + rng.IntN(3)
				}
			}
			if dispatched && !c.IncidentOpen() {
				closed = true
				break
			}
		}

		if !dispatched {
			t.Fatalf("seed %d (inject=%d crash=%d execBefore=%v): incident lost -- never dispatched",
				seed, injectStep, crashStep, executeBeforeCrash)
		}
		if n := dispatcher.callCount(); n != 1 {
			t.Fatalf("seed %d (inject=%d crash=%d execBefore=%v): dispatched %d times across restart, want exactly one",
				seed, injectStep, crashStep, executeBeforeCrash, n)
		}
		if !closed {
			t.Fatalf("seed %d: incident never closed after recovery", seed)
		}
	})
}
