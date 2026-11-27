package runtime

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// The page and external-incident entries open an incident only after the
// previous incident's work (its closing turn, cleanup and source publication)
// finished, like every other opener. A page that arrives meanwhile stays in
// the inbox and opens the next incident once that work is done.

// holdPriorReflection makes c look like the previous incident's queued
// reflection is running and returns a function that finishes it.
func holdPriorReflection(c *Controller) func() {
	c.mu.Lock()
	c.broker = &publishingQueue{release: make(chan struct{})}
	c.reflections.running, c.reflections.rescan, c.reflections.unknown = "previous", false, false
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		c.reflections.running = ""
		c.mu.Unlock()
	}
}

func TestAPageWaitsForThePreviousIncidentsWork(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, _, reads := pageController(t, now)
	finish := holdPriorReflection(c)
	for range 3 {
		c.readPage(context.Background(), now, nil)
		if c.incidentOpen || c.currentIncidentRequest != nil {
			t.Fatal("a page opened a second incident while the previous one's reflection ran")
		}
	}
	if c.lastPageID != "" {
		t.Fatalf("the blocked page was accepted: %q", c.lastPageID)
	}
	finish()
	c.readPage(context.Background(), now, nil)
	if !c.incidentOpen || c.currentIncidentRequest == nil || c.currentIncidentRequest.Page != syntheticPage {
		t.Fatalf("the retained page did not open the next incident: reads=%d", *reads)
	}
}

func TestAnExternalIncidentWaitsForThePreviousIncidentsWork(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := testControllerConfig()
	cfg.ExternalIncidentsOnly = true
	c, err := NewController(cfg, nil, externalUnavailableSnapshots{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	finish := holdPriorReflection(c)
	alert := ExternalIncident{ID: "external-next", Namespace: cfg.Namespace, Message: "investigate"}
	if err := c.openExternalIncident(now, alert); !errors.Is(err, ErrExternalIncidentBusy) {
		t.Fatalf("external incident during the previous reflection: err=%v open=%v", err, c.incidentOpen)
	}
	if c.incidentOpen {
		t.Fatal("a second incident opened while the previous one's reflection ran")
	}
	finish()
	if err := c.openExternalIncident(now, alert); err != nil || !c.incidentOpen || c.currentIncidentRequest.IncidentID != alert.ID {
		t.Fatalf("the retained alert did not open the next incident: %v", err)
	}
}

// A controller restart while the previous incident's reflection runs: the
// restored controller rereads the durable queue, and the waiting opener opens
// the next incident only after the reflection finished.
func TestTheNextIncidentWaitsForAReflectionAcrossARestart(t *testing.T) {
	w := newLateWorld(t, false)
	queue := &publishingQueue{release: make(chan struct{})}
	t.Cleanup(func() { close(queue.release) })
	w.c.broker = queue
	w.noWait = true
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
	restarted, err := NewController(w.c.config, []sdk.Detector{w.cond, w.late}, staticProvider{}, &recordingDispatcher{}, w.now)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return w.now }
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	// The durable queue still holds the reflection the old process ran.
	again := &publishingQueue{release: make(chan struct{}), queued: slices.Clone(queue.queued)}
	restarted.broker = again
	w.c = restarted
	for range 10 {
		w.step()
		if id := w.incidentID(); id != "" && id != first {
			t.Fatal("the next incident opened while the previous reflection ran after a restart")
		}
	}
	if !again.running.Load() {
		t.Fatal("the restored controller did not resume the queued reflection")
	}
	close(again.release)
	w.drive(30, func() bool { id := w.incidentID(); return id != "" && id != first })
	if id := w.incidentID(); id == "" || id == first {
		t.Fatalf("the next incident did not open after the reflection: %q", id)
	}
}

// Only a handover keeps a closed incident's still-active alerts waiting for
// the next incident. A page-opened incident that closes unverified after its
// responder returned acknowledges its alerts as before: they do not reopen
// at once as a new incident.
func TestAnUnverifiedPageCloseAcknowledgesItsAlerts(t *testing.T) {
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
}
