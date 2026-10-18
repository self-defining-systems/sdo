package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
)

// Dimension: induced crash/restart at a randomized point around emit/persist.
//
// A firing incident detector drives the telemetry stream while the controller
// is crashed (discarded and rebuilt on the same durable state and the same
// firing-stream path) at a seed-chosen step, with the durable state persisted
// before the crash or not. The invariant is that detector-firing event ids stay
// stable across the crash: because an event id is a pure function of durable
// state (detector, fingerprint, event kind, per-finding transition counter), a
// replay after a crash-before-persist re-derives the same id and the sink drops
// the duplicate, while a restart after a persisted state re-emits nothing. No
// matter where the crash lands, the activation appears exactly once and no two
// records in the stream share an event id.
func TestChaosTelemetryEventIDsStableAcrossCrashPoint(t *testing.T) {
	sweepChaosSeeds(t, func(t *testing.T, seed uint64) {
		ctx := context.Background()
		rng := newChaosRNG(seed)
		path := filepath.Join(t.TempDir(), "detector-firings.jsonl")
		client := fake.NewSimpleClientset()

		build := func() *Controller {
			sink, err := NewFileFiringSink(path, 1<<20)
			if err != nil {
				t.Fatalf("new sink: %v", err)
			}
			cause := newChaosIncidentDetector("cause")
			cause.active.set(true) // the fault is still present across the crash
			controller := telemetryController(
				t, sink, &recordingDispatcher{requests: make(chan IncidentRequest, 4)}, cause,
			)
			if err := controller.AttachStateStore(ctx, NewConfigMapStateStore(client, "demo", "state")); err != nil {
				t.Fatalf("attach store: %v", err)
			}
			return controller
		}

		crashAfter := 1 + rng.IntN(4)
		persistBeforeCrash := rng.IntN(2) == 0

		first := build()
		for s := 0; s <= crashAfter; s++ {
			step(t, first, s)
		}
		if persistBeforeCrash {
			if err := first.PersistState(ctx); err != nil {
				t.Fatalf("persist before crash: %v", err)
			}
		}

		// Crash: the first controller vanishes. A fresh one reopens the same
		// stream and the same durable state.
		second := build()
		for s := crashAfter + 1; s <= crashAfter+4; s++ {
			step(t, second, s)
		}
		if err := second.PersistState(ctx); err != nil {
			t.Fatalf("persist after restart: %v", err)
		}

		records := readStream(t, path)
		if len(records) == 0 {
			t.Fatalf("seed %d: no telemetry recorded", seed)
		}
		seen := map[string]int{}
		activations := 0
		for _, r := range records {
			seen[r.EventID]++
			if r.Event == FiringActivated && r.DetectorID == "cause" {
				activations++
			}
		}
		for id, n := range seen {
			if n > 1 {
				t.Fatalf("seed %d (crashAfter=%d persist=%v): event id %s emitted %d times -- unstable across crash",
					seed, crashAfter, persistBeforeCrash, id, n)
			}
		}
		if activations != 1 {
			t.Fatalf("seed %d (crashAfter=%d persist=%v): cause activated %d times across the crash, want exactly one",
				seed, crashAfter, persistBeforeCrash, activations)
		}
	})
}
