package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Dimension: a detector evaluation lands late (pull-before-act / late findings).
//
// A health detector opens the incident and dispatches a responder, which is
// then held in flight for a seed-chosen window. A learned incident-class
// detector's evidence appears a seed-chosen number of steps *after* the
// responder launched -- the exact situation the responder's
// `--late-findings pull` is built for, where the immutable incident request has
// already shipped.
//
// Invariants, no matter how long after launch the evidence shows up or how long
// the responder is held: the late activation is recorded exactly once, is
// classified `after_dispatch`, and carries the open incident id -- so it is
// surfaced as a pullable late finding rather than silently dropped or
// misattributed to a fresh incident.
func TestChaosLateFindingAfterDispatchIsSurfacedExactlyOnce(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		rng := newChaosRNG(seed)
		health := newChaosHealthDetector("health")
		late := newChaosIncidentDetector("late")
		dispatcher := &blockingDispatcher{
			requests: make(chan IncidentRequest, 8),
			release:  make(chan struct{}),
		}
		sink := &memorySink{}
		controller, err := NewController(
			testControllerConfig(), []sdk.Detector{health, late},
			staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, dispatcher, chaosStart,
		)
		if err != nil {
			t.Fatalf("new controller: %v", err)
		}
		controller.SetFiringSink(sink)
		var now time.Time
		controller.now = func() time.Time { return now }
		jitter := newChaosRNG(seed ^ 0x5eed5eed5eed5eed)
		controller.dispatchJitter = jitter.Float64

		health.active.set(true) // health fault present from the start -> early dispatch
		lateLag := rng.IntN(5)  // steps after dispatch before the late signal turns on
		holdFor := 2 + rng.IntN(5)

		dispatched := false
		released := false
		var dispatchStep, releaseStep int
		incidentID := ""
		lateOnStep := -1

		for step := 0; step < chaosMaxSteps; step++ {
			now = time.Unix(int64(step+1), 0).UTC()
			if dispatched && lateOnStep < 0 && step >= dispatchStep+lateLag {
				late.active.set(true)
				lateOnStep = step
			}
			if err := controller.StepEvents(context.Background(), now, nil); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if !dispatched {
				if effect, ok := controller.PendingDispatchEffect(); ok {
					if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
						t.Fatalf("execute dispatch effect: %v", err)
					}
					incidentID = awaitRequest(t, dispatcher.requests).IncidentID
					dispatched = true
					dispatchStep = step
				}
				continue
			}
			if !released && lateOnStep >= 0 && step >= lateOnStep+holdFor {
				close(dispatcher.release)
				released = true
				releaseStep = step
				awaitDispatchCompletionQueued(t, controller)
			}
			// Done once the late finding has had time to persist and the responder
			// completion has been processed.
			if released && len(sink.events(FiringActivated, "late")) > 0 && step > releaseStep {
				break
			}
		}
		if !released && dispatched {
			close(dispatcher.release)
		}
		if !dispatched {
			t.Fatalf("seed %d: health fault never dispatched", seed)
		}

		activated := sink.events(FiringActivated, "late")
		if len(activated) != 1 {
			t.Fatalf("seed %d: late activation recorded %d times, want exactly one: %+v", seed, len(activated), activated)
		}
		rec := activated[0]
		if rec.DispatchRelation != RelationAfterDispatch {
			t.Fatalf("seed %d: late finding (on at step %d, dispatch %d) relation=%s, want after_dispatch",
				seed, lateOnStep, dispatchStep, rec.DispatchRelation)
		}
		if rec.IncidentID != incidentID {
			t.Fatalf("seed %d: after_dispatch activation incident id %q != open incident %q", seed, rec.IncidentID, incidentID)
		}
		// A late finding must never be mistaken for a second batched incident.
		if batched := sink.events(FiringBatched, "late"); len(batched) != 0 {
			t.Fatalf("seed %d: late (after-dispatch) finding was batched into a dispatch: %+v", seed, batched)
		}
	})
}
