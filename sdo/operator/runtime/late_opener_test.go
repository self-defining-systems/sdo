package runtime

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// A user test that starts failing after the responder returned is a new
// incident. These tests cover what happens to it and to the incident that is
// still open: the first closes on its own alert set, the test opens the next
// incident, and two incidents never write live objects at once.

// lateWorld steps a controller whose first incident opened on a Kubernetes
// condition and whose responder returned; late is a user test outside its set.
// It lives in a synctest bubble: each step ends only when every background
// goroutine has finished or is parked (on a held publication, say), so what a
// step observes never depends on the scheduler. A step outside a bubble panics.
type lateWorld struct {
	t      *testing.T
	c      *Controller
	now    time.Time
	closed []IncidentClosure
	late   *switchedProbe
	cond   *switchedProbe
}

func newLateWorld(t *testing.T, conditionFailing bool) *lateWorld {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	cond := &switchedProbe{id: memory.KubernetesConditionDetectorIDs()[0], scenario: "not-ready"}
	cond.failing.Store(conditionFailing)
	late := &switchedProbe{id: memory.TrafficDetectorPrefix + "late", scenario: "clone"}
	c, _ := alertSetController(t, now, cond, late)
	opening := cond.finding()
	c.currentIncidentRequest.Findings, c.currentIncidentRequest.OpeningKeys = []sdk.Finding{opening}, findingKeys([]sdk.Finding{opening})
	request := c.currentIncidentRequest
	request.SchemaVersion, request.Namespace = ProtocolSchemaVersion, "demo"
	request.SourceCommit, request.DeployedCommit, request.RepositoryWorktree = "source", "deployed", "synthetic-worktree"
	request.RepositoryBaseCommit, request.CancellationToken, request.RepairPolicy = "source", "synthetic-token", "commit"
	c.currentIncidentRequest.DetectorHistory = []DetectorEvaluation{{DetectorID: cond.id, EvaluatedAt: now.Add(-3 * time.Second), Status: DetectorEvaluationFiring}}
	action := &c.currentIncidentResult.RepairActions[0]
	action.Target, action.Summary, action.Details = "Deployment/web", "restart web", "synthetic"
	action.StartedAt, action.CompletedAt = now.Add(-2*time.Second), now.Add(-time.Second)
	c.incidentFindingKeys = findingKeys([]sdk.Finding{opening})
	w := &lateWorld{t: t, c: c, now: now, late: late, cond: cond}
	c.now = func() time.Time { return w.now }
	c.OnIncidentClosed = func(closure IncidentClosure) { w.closed = append(w.closed, closure) }
	return w
}

func (w *lateWorld) step() {
	w.t.Helper()
	if err := w.c.Step(context.Background(), w.now, nil); err != nil {
		w.t.Fatal(err)
	}
	synctest.Wait()
	w.now = w.now.Add(time.Second)
}

// runUntil steps at most limit times until done holds.
func (w *lateWorld) runUntil(limit int, done func() bool) {
	w.t.Helper()
	for range limit {
		if done() {
			return
		}
		w.step()
	}
}

// drive is runUntil that also runs the broker effects a step leaves pending.
func (w *lateWorld) drive(limit int, done func() bool) {
	w.t.Helper()
	for range limit {
		if done() {
			return
		}
		w.step()
		if effect, ok := w.c.PendingClosureEffect(); ok {
			if err := w.c.ExecuteClosureEffect(context.Background(), effect); err != nil {
				w.t.Fatal(err)
			}
			synctest.Wait()
		}
		if effect, ok := w.c.PendingClosureAcknowledgmentEffect(); ok {
			if err := w.c.ExecuteClosureAcknowledgmentEffect(context.Background(), effect); err != nil {
				w.t.Fatal(err)
			}
			synctest.Wait()
		}
	}
}

func (w *lateWorld) incidentID() string {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	if w.c.currentIncidentRequest == nil {
		return ""
	}
	return w.c.currentIncidentRequest.IncidentID
}

func (w *lateWorld) openingKeys() []string {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	if w.c.currentIncidentRequest == nil {
		return nil
	}
	return slices.Clone(w.c.currentIncidentRequest.OpeningKeys)
}

// A fault that keeps the incident's own alert failing and breaks another user
// test after the responder returned: nothing works the incident any more, so
// it closes escalated (no learning credit) and the next incident opens on
// every alert still failing.
func TestAnEscalatedIncidentHandsItsFailingAlertsToTheNextIncident(t *testing.T) {
	for _, withBroker := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "broker"}[withBroker], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newLateWorld(t, true)
				w.c.config.VerificationTimeout = 3 * time.Second
				if withBroker {
					queue := &publishingQueue{release: make(chan struct{})}
					close(queue.release)
					w.c.broker = queue
				}
				first := w.incidentID()
				w.late.failing.Store(true)
				w.drive(30, func() bool { return len(w.closed) > 0 })
				if len(w.closed) != 1 {
					t.Fatal("the escalated incident never closed while a new opener waited")
				}
				closed := w.closed[0]
				if closed.Request.IncidentID != first || closed.EscalatedAt.IsZero() || len(closed.FinalDetectorStates) != 0 || closed.HealthClearedAt != nil ||
					len(closed.Request.JoinedKeys) != 0 {
					t.Fatalf("want an escalated, unverified close of the first incident on its own set: %+v", closed)
				}
				if closed.Result == nil || !slices.ContainsFunc(closed.Result.UnresolvedFindings, func(f sdk.Finding) bool { return f.DetectorID == w.cond.id }) {
					t.Fatalf("the escalated close must name its unresolved alert: %+v", closed.Result)
				}
				w.drive(30, func() bool { return w.incidentID() != "" && w.incidentID() != first })
				want := findingKeys([]sdk.Finding{w.cond.finding(), w.late.finding()})
				if got := w.openingKeys(); w.incidentID() == first || !slices.Equal(got, want) {
					t.Fatalf("next incident opening keys = %v, want %v (every alert still failing)", got, want)
				}
			})
		})
	}
}

// An escalated incident with no waiting opener stays open until its alerts
// clear.
func TestAnEscalatedIncidentWithNothingWaitingStaysOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, true)
		w.c.config.VerificationTimeout = 3 * time.Second
		w.runUntil(30, func() bool { return len(w.closed) > 0 })
		if len(w.closed) != 0 || !w.c.IncidentOpen() || w.c.escalatedAt.IsZero() {
			t.Fatalf("closed=%d open=%v escalated=%s", len(w.closed), w.c.IncidentOpen(), w.c.escalatedAt)
		}
	})
}

// The waiting opener survives a restart: the batch is persisted, and the
// restarted controller opens the next incident on it once the first closes.
func TestAWaitingOpenerSurvivesARestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		first := w.incidentID()
		w.late.failing.Store(true)
		w.step()
		if len(w.c.batcher.Snapshot().Findings) != 1 {
			t.Fatalf("the late user test did not wait in the batcher: %v", w.c.batcher.Snapshot().Findings)
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
		if got := restarted.batcher.Snapshot().Findings; len(got) != 1 || findingKey(got[0]) != findingKey(w.late.finding()) {
			t.Fatalf("restored batch = %v", got)
		}
		restarted.OnIncidentClosed = func(closure IncidentClosure) { w.closed = append(w.closed, closure) }
		w.c = restarted
		w.runUntil(30, func() bool { return w.incidentID() != "" && w.incidentID() != first })
		if len(w.closed) != 1 || !w.closed[0].EscalatedAt.IsZero() || len(w.closed[0].FinalDetectorStates) == 0 {
			t.Fatalf("the first incident did not close verified: %+v", w.closed)
		}
		if got := w.openingKeys(); !slices.Equal(got, findingKeys([]sdk.Finding{w.late.finding()})) {
			t.Fatalf("next incident opening keys = %v", got)
		}
	})
}

// A user test that fails, clears and fails again around the first incident's
// close opens at most one more incident and never changes the first outcome.
func TestAFlappingUserTestOpensAtMostOneMoreIncident(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newLateWorld(t, false)
		first := w.incidentID()
		seen := map[string]bool{first: true}
		for i := range 40 {
			w.late.failing.Store(i == 0 || i == 1 || i >= 4)
			w.step()
			if id := w.incidentID(); id != "" {
				seen[id] = true
			}
		}
		if len(seen) > 2 {
			t.Fatalf("a flapping user test opened %d incidents: %v", len(seen)-1, seen)
		}
		if len(w.closed) == 0 || w.closed[0].Request.IncidentID != first || !w.closed[0].EscalatedAt.IsZero() ||
			len(w.closed[0].Request.JoinedKeys) != 0 || len(w.closed[0].FinalDetectorStates) == 0 {
			t.Fatalf("the first incident's outcome changed: %+v", w.closed)
		}
	})
}

// After an applied playbook, under after_playbook=escalate or =wait, a user
// test that fails while the stage owns the repair joins the alert set; one
// that fails after the stage escalated waits for the next incident, which the
// escalated incident hands it to. It is never lost.
func TestJoinersAfterAPlaybookWithoutAnAgentAreNeverLost(t *testing.T) {
	for _, policy := range []string{settings.AfterPlaybookEscalate, settings.AfterPlaybookWait} {
		t.Run(policy, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{}, policy)
			c.config.VerificationTimeout = time.Second
			joined := (&switchedProbe{id: memory.TrafficDetectorPrefix + "joined", scenario: "browse"}).finding()
			c.admitActivation(t.Context(), joined, world.clock.Now(), sdk.DetectorSpec{})
			if !slices.Contains(c.currentIncidentRequest.JoinedKeys, findingKey(joined)) {
				t.Fatal("a user test did not join while the stage owned the repair")
			}
			for range 600 {
				if !c.escalatedAt.IsZero() {
					break
				}
				autoStep(t, c, world.clock, time.Second)
			}
			if c.escalatedAt.IsZero() {
				t.Fatal("the stage never escalated")
			}
			late := (&switchedProbe{id: memory.TrafficDetectorPrefix + "late", scenario: "write"}).finding()
			c.admitActivation(t.Context(), late, world.clock.Now(), sdk.DetectorSpec{})
			if slices.Contains(c.currentIncidentRequest.JoinedKeys, findingKey(late)) {
				t.Fatal("a user test joined an escalated incident")
			}
			for range 10 {
				if closed.Result != nil {
					break
				}
				autoStep(t, c, world.clock, time.Second)
			}
			if closed.Result == nil || closed.EscalatedAt.IsZero() {
				t.Fatalf("the escalated incident did not close with a new opener waiting: %+v", closed)
			}
			if !slices.ContainsFunc(c.batcher.Snapshot().Findings, func(f sdk.Finding) bool { return findingKey(f) == findingKey(late) }) {
				t.Fatalf("the late user test was lost: batch %v", c.batcher.Snapshot().Findings)
			}
		})
	}
}

// publishingQueue is a broker whose acknowledged closures queue a reflection
// that carries a source publication; the publication runs until released.
type publishingQueue struct {
	riskBroker
	mu       sync.Mutex
	queued   []string
	release  chan struct{}
	running  atomic.Bool
	finished atomic.Bool
}

func (q *publishingQueue) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	receipt, err := q.riskBroker.ProcessClosure(ctx, closure)
	receipt.OutcomeCommit, receipt.AckToken = "synthetic-outcome", "synthetic-ack"
	return receipt, err
}

func (q *publishingQueue) AcknowledgeClosure(_ context.Context, receipt ClosureReceipt) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queued = append(q.queued, receipt.IncidentID)
	return nil
}

func (q *publishingQueue) QueuedReflections() ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return slices.Clone(q.queued), nil
}

func (q *publishingQueue) ProcessQueuedReflection(_ context.Context, id string) error {
	q.running.Store(true)
	<-q.release
	q.mu.Lock()
	q.queued = slices.DeleteFunc(q.queued, func(queued string) bool { return queued == id })
	q.mu.Unlock()
	q.running.Store(false)
	q.finished.Store(true)
	return nil
}

func (q *publishingQueue) AbandonQueuedReflection(context.Context, string, string) error { return nil }
func (q *publishingQueue) QueuedReflectionRolloutPending(string) (bool, error)           { return false, nil }
func (q *publishingQueue) QueuedReflectionServingCommit(context.Context, string) (string, error) {
	return "", nil
}
