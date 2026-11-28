package runtime

import (
	"slices"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// A controller restart while the previous incident's closing turn runs: the
// restored controller rereads the durable queue and resumes the closing turn,
// and the waiting opener opens the next incident without waiting for it.
func TestTheNextIncidentOpensDuringAResumedClosingTurnAfterARestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		w.c.incidentDetectedAt = w.now.Add(-AlertJoinWindow - time.Second) // the late test waits for the next incident
		queue := &publishingQueue{release: make(chan struct{})}
		t.Cleanup(func() { close(queue.release) })
		w.c.broker = queue
		first := w.incidentID()
		w.late.failing.Store(true)
		w.drive(60, queue.running.Load)
		if !queue.running.Load() {
			t.Fatal("the first closure's reflection never started")
		}
		state, err := w.c.ExportState()
		if err != nil {
			t.Fatal(err)
		}
		restarted, err := NewController(w.c.config, []sdk.Detector{w.cond, w.late}, staticProvider{}, &answeringDispatcher{}, w.now)
		if err != nil {
			t.Fatal(err)
		}
		restarted.now = func() time.Time { return w.now }
		if err := restarted.RestoreState(state); err != nil {
			t.Fatal(err)
		}
		// The durable queue still holds the reflection the old process ran.
		again := &publishingQueue{release: make(chan struct{}), queued: slices.Clone(queue.queued)}
		t.Cleanup(func() { close(again.release) })
		restarted.broker = again
		w.c = restarted
		w.drive(30, func() bool { id := w.incidentID(); return id != "" && id != first })
		if id := w.incidentID(); id == "" || id == first {
			t.Fatalf("the next incident did not open during the resumed closing turn: %q", id)
		}
		w.drive(5, again.running.Load)
		if !again.running.Load() {
			t.Fatal("the restored controller did not resume the queued reflection")
		}
	})
}

// Only a handover keeps a closed incident's still-active alerts waiting for
// the next incident. A page-opened incident that closes unverified after its
// responder returned acknowledges its alerts as before: they do not reopen
// at once as a new incident.
func TestAnUnverifiedPageCloseAcknowledgesItsAlerts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, true)
		w.c.config.VerificationTimeout = 3 * time.Second
		queue := &publishingQueue{release: make(chan struct{})}
		close(queue.release)
		w.c.broker = queue
		// The page opened the incident; the alert it carries still fails.
		joined := w.cond.finding()
		w.c.mu.Lock()
		request := w.c.currentIncidentRequest
		request.Page, request.OpeningKeys, request.JoinedKeys = syntheticPage, nil, findingKeys([]sdk.Finding{joined})
		// A batch entry of the same alert, which acknowledgment owns.
		w.c.batcher.AddAt(joined, w.now)
		w.c.mu.Unlock()
		first := w.incidentID()
		w.drive(60, func() bool {
			w.c.mu.Lock()
			defer w.c.mu.Unlock()
			return len(w.closed) == 1 && w.c.closureState == contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED && len(queue.closures()) == 1
		})
		if len(queue.closures()) != 1 || w.c.closureState != contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED {
			t.Fatalf("the page close was not acknowledged: state=%s", w.c.closureState)
		}
		if len(w.closed) != 1 || w.closed[0].HealthClearedAt != nil || len(w.closed[0].FinalDetectorStates) != 0 {
			t.Fatalf("the page did not close unverified: %+v", w.closed)
		}
		for range 10 {
			w.step()
		}
		if id := w.incidentID(); id != "" && id != first {
			t.Fatalf("an unverified page close handed its alert to a new incident %q", id)
		}
		key := findingKey(joined)
		for _, finding := range w.c.batcher.Snapshot().Findings {
			if findingKey(finding) == key {
				t.Fatal("an unverified page close left its alert waiting in the batcher")
			}
		}
	})
}
