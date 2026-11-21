package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// requeueingChanges delivers its changes once, like the cache.
type requeueingChanges struct{ pending []ObjectChange }

func (q *requeueingChanges) TakeObjectChanges() []ObjectChange {
	changes := q.pending
	q.pending = nil
	return changes
}

// failingDiffView answers every comparison with err and counts them.
type failingDiffView struct {
	*sdktest.ReplayReadView
	err   error
	calls *int
}

func (v failingDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	*v.calls++
	return program.Diff{}, v.err
}

type driftRetryWorld struct {
	c      *Controller
	start  time.Time
	calls  int
	errors []string
}

func newDriftRetryWorld(t *testing.T, readErr error) *driftRetryWorld {
	t.Helper()
	w := &driftRetryWorld{start: time.Unix(1000, 0)}
	config := testControllerConfig()
	config.Namespace = "app"
	c, err := NewController(config, nil, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "app"}}, &recordingDispatcher{requests: make(chan IncidentRequest, 1)}, w.start)
	if err != nil {
		t.Fatal(err)
	}
	queue := &requeueingChanges{pending: []ObjectChange{objectChange(t, driftService(8080), driftService(9999), "v1", "Service")}}
	c.ConfigureDrift(DriftControllerConfig{Events: queue, View: func(s sdk.DetectionContext) program.ReadView {
		return failingDiffView{ReplayReadView: &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}, err: readErr, calls: &w.calls}
	}})
	c.OnError = func(err error) { w.errors = append(w.errors, err.Error()) }
	w.c = c
	return w
}

func (w *driftRetryWorld) stepAt(t *testing.T, elapsed time.Duration) {
	t.Helper()
	if err := w.c.StepEvents(context.Background(), w.start.Add(elapsed), nil); err != nil {
		t.Fatal(err)
	}
}

// A classification that cannot be read right now is retried on a growing
// delay, not on every step, and is given up (and said so) once its bound
// passes, so a change that can never classify stops costing a snapshot per
// step for the life of the controller.
func TestDriftClassificationErrorBacksOffThenIsAbandoned(t *testing.T) {
	w := newDriftRetryWorld(t, errors.New("object not captured"))
	w.stepAt(t, 0)
	if w.calls != 1 {
		t.Fatalf("first step classified %d times, want 1", w.calls)
	}
	w.stepAt(t, time.Second)
	if w.calls != 1 {
		t.Fatalf("a step inside the first backoff classified again: %d calls", w.calls)
	}
	w.stepAt(t, driftRetryPolicy.First)
	if w.calls != 2 {
		t.Fatalf("the step at the first backoff made %d calls, want 2", w.calls)
	}
	for elapsed := time.Duration(0); elapsed <= driftRetryPolicy.Bound+time.Minute; elapsed += time.Second {
		w.stepAt(t, elapsed)
	}
	attempts := w.calls
	if attempts > 12 {
		t.Fatalf("%d classification attempts in %s: the retry is not backing off", attempts, driftRetryPolicy.Bound)
	}
	abandoned := 0
	for _, message := range w.errors {
		if strings.Contains(message, "drift_classification_abandoned") {
			abandoned++
		}
	}
	if abandoned != 1 {
		t.Fatalf("abandoned reported %d times, want once: %v", abandoned, w.errors)
	}
	w.stepAt(t, driftRetryPolicy.Bound+10*time.Minute)
	if w.calls != attempts {
		t.Fatalf("an abandoned change was classified again: %d -> %d calls", attempts, w.calls)
	}
}

// A failure no retry can fix (the same inputs fail the same way) is reported
// once and never re-queued.
func TestDriftPermanentClassificationErrorIsNotRetried(t *testing.T) {
	w := newDriftRetryWorld(t, fmt.Errorf("%w: read budget exceeded", ErrDispatchPermanent))
	for _, elapsed := range []time.Duration{0, time.Second, time.Minute, 10 * time.Minute} {
		w.stepAt(t, elapsed)
	}
	if w.calls != 1 || len(w.errors) != 1 {
		t.Fatalf("permanent error: %d attempts, %d reports (%v), want 1 and 1", w.calls, len(w.errors), w.errors)
	}
}

// An object change is not a write SDO knows about, and neither is its
// deferred retry: neither restarts the recovery stability window, or an
// incident whose alerts cleared never closes while the cluster keeps changing
// or a classification keeps failing.
func TestObjectChangesAndDriftRetriesDoNotRestartRecoveryStability(t *testing.T) {
	w := newDriftRetryWorld(t, errors.New("accepted source is unavailable"))
	queue := w.c.drift.Events.(*requeueingChanges)
	stable := w.start.Add(time.Second)
	armed := func() {
		w.c.mu.Lock()
		w.c.closureStableSince, w.c.recoverySafetyLost = stable, false
		w.c.mu.Unlock()
	}
	window := func() (time.Time, bool) {
		w.c.mu.Lock()
		defer w.c.mu.Unlock()
		return w.c.closureStableSince, w.c.recoverySafetyLost
	}
	armed()
	w.stepAt(t, 2*time.Second)
	if since, lost := window(); !since.Equal(stable) || lost {
		t.Fatalf("an object change restarted the window: since=%s lost=%v", since, lost)
	}
	queue.pending = nil
	for _, elapsed := range []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second} {
		armed()
		before := w.calls
		w.stepAt(t, elapsed)
		if w.calls == before {
			t.Fatalf("t+%s: the deferred retry did not run", elapsed)
		}
		if since, lost := window(); !since.Equal(stable) || lost {
			t.Fatalf("t+%s: a deferred retry restarted the window: since=%s lost=%v", elapsed, since, lost)
		}
	}
}
