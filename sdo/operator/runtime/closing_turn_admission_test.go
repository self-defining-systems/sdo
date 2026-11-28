package runtime

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
)

// The previous incident's closing turn never holds the next incident: users
// harmed while it runs get an incident and a responder at once. Only its source
// deploy waits, and the closing session's live writes stay behind its own
// incident's fence, so the two incidents never write live objects at once.

// incidentGates gives each incident its own mutation gate, as the session
// mount does: one directory per incident ID.
func incidentGates(t *testing.T) func(string) (*apihistory.MutationGate, error) {
	root := t.TempDir()
	return func(id string) (*apihistory.MutationGate, error) {
		return apihistory.PrepareMutationGate(filepath.Join(root, fmt.Sprintf("%x", sha256.Sum256([]byte(id)))))
	}
}

// answeringDispatcher returns a completed result at once.
type answeringDispatcher struct{ calls atomic.Int32 }

func (d *answeringDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.calls.Add(1)
	return completedResult(request.IncidentID), nil
}

// driveAll is lateWorld.drive that also runs the workspace and dispatch
// effects, so the next incident's responder starts and returns.
func (w *lateWorld) driveAll(limit int, done func() bool) {
	w.t.Helper()
	for range limit {
		if done() {
			return
		}
		w.drive(1, func() bool { return false })
		if effect, ok := w.c.PendingWorkspaceEffect(); ok {
			if err := w.c.ExecuteWorkspaceEffect(context.Background(), effect); err != nil {
				w.t.Fatal(err)
			}
			synctest.Wait()
		}
		if effect, ok := w.c.PendingDispatchEffect(); ok {
			if err := w.c.ExecuteDispatchEffect(context.Background(), effect); err != nil {
				w.t.Fatal(err)
			}
			synctest.Wait()
		}
	}
}

// closeFirstIntoItsClosingTurn closes the late world's first incident verified
// and leaves its queued closing turn running.
func closeFirstIntoItsClosingTurn(t *testing.T, w *lateWorld, running func() bool) IncidentClosure {
	t.Helper()
	w.drive(60, running)
	if !running() || len(w.closed) != 1 {
		t.Fatalf("the first incident did not close into its closing turn: closed=%d", len(w.closed))
	}
	first := w.closed[0]
	if !first.EscalatedAt.IsZero() || first.HealthClearedAt == nil || len(first.Request.JoinedKeys) != 0 {
		t.Fatalf("the first incident did not close verified on its own alert set: %+v", first)
	}
	return first
}

func TestAnIncidentOpensAndDispatchesWhileThePreviousClosingTurnRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		queue := &publishingQueue{release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(queue.release) })
		defer release()
		w.c.broker = queue
		dispatcher := &answeringDispatcher{}
		w.c.dispatcher = dispatcher
		first := closeFirstIntoItsClosingTurn(t, w, queue.running.Load)
		w.late.failing.Store(true)
		w.driveAll(10, func() bool { id := w.incidentID(); return id != "" && id != first.Request.IncidentID })
		if !queue.running.Load() {
			t.Fatal("the closing turn ended before the test could observe it")
		}
		if id := w.incidentID(); id == "" || id == first.Request.IncidentID {
			t.Fatal("no incident opened while the previous closing turn ran")
		}
		if got, want := w.openingKeys(), findingKeys([]sdk.Finding{w.late.finding()}); !slices.Equal(got, want) {
			t.Fatalf("opening keys = %v, want %v", got, want)
		}
		w.driveAll(5, func() bool { return dispatcher.calls.Load() > 0 })
		if dispatcher.calls.Load() != 1 {
			t.Fatalf("the new incident's responder was dispatched %d times", dispatcher.calls.Load())
		}
		// The prior incident's outcome is the one its closure committed.
		if closures := queue.closures(); len(closures) != 1 || closures[0].Request.IncidentID != first.Request.IncidentID {
			t.Fatalf("closures during the closing turn = %d", len(closures))
		}
		release()
		synctest.Wait()
	})
}

func TestAPageOpensAnIncidentWhileThePreviousClosingTurnRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		queue := &publishingQueue{release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(queue.release) })
		defer release()
		w.c.broker = queue
		w.c.dispatcher = &answeringDispatcher{}
		first := closeFirstIntoItsClosingTurn(t, w, queue.running.Load)
		pages := 0
		w.c.ExternalIncidents = func(context.Context) (*ExternalIncident, error) {
			pages++
			return &ExternalIncident{ID: "page-next", Namespace: "demo", Message: syntheticPage}, nil
		}
		w.step()
		w.c.mu.Lock()
		request := w.c.currentIncidentRequest
		opened := w.c.incidentOpen && request != nil && request.IncidentID != first.Request.IncidentID && request.Page == syntheticPage
		w.c.mu.Unlock()
		if pages == 0 || !opened || !queue.running.Load() {
			t.Fatalf("a page did not open an incident during the closing turn: reads=%d opened=%v", pages, opened)
		}
		release()
		synctest.Wait()
	})
}

func TestAnExternalIncidentOpensWhileThePreviousClosingTurnRuns(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	cfg := testControllerConfig()
	cfg.ExternalIncidentsOnly = true
	c, err := NewController(cfg, nil, externalUnavailableSnapshots{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.broker = &publishingQueue{release: make(chan struct{})}
	c.reflections.running, c.reflections.queued = "previous", []string{"previous"}
	alert := ExternalIncident{ID: "external-next", Namespace: cfg.Namespace, Message: "investigate"}
	if err := c.openExternalIncident(now, alert); err != nil || !c.incidentOpen || c.currentIncidentRequest.IncidentID != alert.ID {
		t.Fatalf("an external incident did not open during the closing turn: %v", err)
	}
}

// One writer: the closing session keeps the previous incident's fence, which
// the next incident never lifts, while the next responder's gate takes writes.
func TestTheClosingSessionStaysFencedWhileTheNextIncidentWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		queue := &publishingQueue{release: make(chan struct{})}
		release := sync.OnceFunc(func() { close(queue.release) })
		defer release()
		w.c.broker = queue
		w.c.dispatcher = &answeringDispatcher{}
		gates := incidentGates(t)
		w.c.incidentGate = gates
		first := closeFirstIntoItsClosingTurn(t, w, queue.running.Load)
		w.late.failing.Store(true)
		w.driveAll(10, func() bool { id := w.incidentID(); return id != "" && id != first.Request.IncidentID })
		next := w.incidentID()
		if next == "" || next == first.Request.IncidentID || !queue.running.Load() {
			t.Fatalf("the next incident did not open during the closing turn: %q", next)
		}
		for range 5 {
			w.step()
		}
		old, err := gates(first.Request.IncidentID)
		if err != nil {
			t.Fatal(err)
		}
		if old.Fenced() == nil {
			t.Fatal("the closing session's incident is no longer fenced while the next incident is open")
		}
		if lease, err := old.Begin(); err == nil {
			_ = lease.Finish(false)
			t.Fatal("the closing session could start a live write while the next incident was open")
		}
		current, err := gates(next)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := current.Begin()
		if err != nil {
			t.Fatalf("the next incident's responder cannot write: %v", err)
		}
		if err := lease.Finish(false); err != nil {
			t.Fatal(err)
		}
		release()
		synctest.Wait()
	})
}

// deployingQueue is a broker whose queued reflection runs the closing turn
// (memory only) until released, then its source deploy, which, like the real
// one, defers while the controller's source gate names work.
type deployingQueue struct {
	riskBroker
	gate    *SourceGate
	release chan struct{}
	mu      sync.Mutex
	queued  []string
	// turns counts closing turns; deferrals and deployed count deploy attempts.
	turns, deferrals, deployed atomic.Int32
	turnDone                   map[string]bool
}

func (q *deployingQueue) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	receipt, err := q.riskBroker.ProcessClosure(ctx, closure)
	receipt.OutcomeCommit, receipt.AckToken = "synthetic-outcome", "synthetic-ack"
	return receipt, err
}

func (q *deployingQueue) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queued = append(q.queued, receipt.IncidentID)
	return nil
}

func (q *deployingQueue) QueuedReflections() ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.queued), nil
}

func (q *deployingQueue) ProcessQueuedReflection(_ context.Context, id string) error {
	q.mu.Lock()
	done := q.turnDone[id]
	q.mu.Unlock()
	if !done {
		q.turns.Add(1)
		<-q.release
		q.mu.Lock()
		q.turnDone[id] = true
		q.mu.Unlock()
	}
	if reason := q.gate.Busy(); reason != "" {
		q.deferrals.Add(1)
		return &SourceDeferredError{Reason: reason}
	}
	q.deployed.Add(1)
	q.mu.Lock()
	q.queued = slices.DeleteFunc(q.queued, func(queued string) bool { return queued == id })
	q.mu.Unlock()
	return nil
}

func (q *deployingQueue) AbandonQueuedReflection(context.Context, string, string) error { return nil }
func (q *deployingQueue) QueuedReflectionRolloutPending(string) (bool, error)           { return false, nil }
func (q *deployingQueue) QueuedReflectionServingCommit(context.Context, string) (string, error) {
	return "", nil
}

// The closing turn's source deploy is the one step that waits for the next
// incident: it defers while that incident is open, without repeating the
// closing turn or retrying while the incident stays open, and deploys once
// that incident ended.
func TestTheClosingTurnsDeployWaitsForTheNextIncident(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		gate := &SourceGate{}
		gate.Set(w.c.sourceDeployBusy)
		queue := &deployingQueue{gate: gate, release: make(chan struct{}), turnDone: map[string]bool{}}
		release := sync.OnceFunc(func() { close(queue.release) })
		defer release()
		w.c.broker = queue
		w.c.dispatcher = &answeringDispatcher{}
		first := closeFirstIntoItsClosingTurn(t, w, func() bool { return queue.turns.Load() == 1 })
		w.late.failing.Store(true)
		w.driveAll(10, func() bool { id := w.incidentID(); return id != "" && id != first.Request.IncidentID })
		next := w.incidentID()
		if next == "" || next == first.Request.IncidentID {
			t.Fatal("the next incident did not open during the closing turn")
		}
		// The closing turn ends while the next incident is open: its deploy waits.
		release()
		synctest.Wait()
		w.driveAll(10, func() bool { return false })
		// One deferral, then no retry while the incident stays open.
		if queue.deferrals.Load() != 1 || queue.deployed.Load() != 0 || !w.c.IncidentOpen() {
			t.Fatalf("deferrals=%d deployed=%d open=%v: the deploy must wait for the open incident, without retrying",
				queue.deferrals.Load(), queue.deployed.Load(), w.c.IncidentOpen())
		}
		if queue.turns.Load() != 1 {
			t.Fatalf("a deferred deploy repeated the closing turn: turns=%d", queue.turns.Load())
		}
		// Once the next incident ends (here an operator reset), the deploy runs.
		w.late.failing.Store(false)
		if id, err := w.c.ResetIncidents(context.Background(), w.now); err != nil || id != next {
			t.Fatalf("reset the next incident: id=%q err=%v", id, err)
		}
		w.driveAll(30, func() bool { return queue.deployed.Load() > 0 })
		if queue.deployed.Load() != 1 || w.c.IncidentOpen() {
			t.Fatalf("deployed=%d open=%v: the deploy did not run after the next incident ended", queue.deployed.Load(), w.c.IncidentOpen())
		}
		if w.closed[0].Request.IncidentID != first.Request.IncidentID || w.closed[0].HealthClearedAt == nil {
			t.Fatalf("the first incident's outcome changed: %+v", w.closed[0])
		}
	})
}
