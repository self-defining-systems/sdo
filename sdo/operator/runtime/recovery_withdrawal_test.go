package runtime

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// verifiedRunningSession is an incident SDO verified and fenced while the
// responder session still runs, before any publication.
func verifiedRunningSession(t *testing.T) (*closureRestartWorld, *apihistory.MutationGate) {
	t.Helper()
	w := newClosureRestartWorld(t)
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := w.controller
	c.incidentGate = func(string) (*apihistory.MutationGate, error) { return gate, nil }
	c.config.ResponseTimeout = 10 * time.Minute
	w.runUntil(t, 10, nil, func() bool { return w.dispatcher.hasStarted() })
	c.mu.Lock()
	c.responderLoop = &ResponderLoop{MutationGate: gate}
	c.mu.Unlock()
	t.Cleanup(func() {
		c.mu.Lock()
		cancel := c.responderCancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		c.Wait()
	})
	w.verify(t, gate)
	return w, gate
}

func (w *closureRestartWorld) verify(t *testing.T, gate *apihistory.MutationGate) {
	t.Helper()
	w.runUntil(t, 60, nil, func() bool { return w.controller.earlyReportPendingLocked() })
	if !w.controller.earlyReportPendingLocked() {
		t.Fatal("recovery was not verified while the session ran")
	}
	if _, err := gate.Begin(); err == nil || err.Error() != "incident_verified" {
		t.Fatalf("a verified incident must refuse writes: %v", err)
	}
}

// refail breaks the alert that opened the incident for a while.
func (w *closureRestartWorld) refail() {
	w.world.healthy.Store(false)
	w.healedAt = w.controller.clock().Add(10 * time.Second)
}

func joinedCondition() sdk.Finding {
	return sdk.Finding{DetectorID: "workload-health", RuleID: "deployment-unready", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "other"}, Summary: "other is unready", Evidence: "synthetic"}
}

func requireOpenForWrites(t *testing.T, w *closureRestartWorld, gate *apihistory.MutationGate) {
	t.Helper()
	c := w.controller
	if !c.IncidentOpen() || c.pendingClosure != nil || c.responderDone || !c.escalatedAt.IsZero() || c.responderLoop == nil {
		t.Fatalf("recovery was not withdrawn from the running session: open=%v pending=%v done=%v escalated=%s", c.IncidentOpen(), c.pendingClosure != nil, c.responderDone, c.escalatedAt)
	}
	if c.incidentVerificationWarning(c.currentIncidentRequest.IncidentID) != nil {
		t.Fatal("the agent is still told that the incident is verified")
	}
	if _, again := c.PendingDispatchEffect(); again {
		t.Fatal("a withdrawn recovery prompted the agent again")
	}
	requireWrite(t, gate)
}

func requireWrite(t *testing.T, gate *apihistory.MutationGate) {
	t.Helper()
	lease, err := gate.Begin()
	if err != nil {
		t.Fatalf("the running session may not write: %v", err)
	}
	if err := lease.Finish(false); err != nil {
		t.Fatal(err)
	}
}

func TestJoinAfterVerificationReopensWritesForTheRunningSession(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	finding := joinedCondition()
	before := len(c.pendingClosure.DetectorTimeline)
	if !c.joinAlertSet(finding, c.clock()) {
		t.Fatal("the alert did not join")
	}
	requireOpenForWrites(t, w, gate)
	if !slices.Contains(c.currentIncidentRequest.JoinedKeys, findingKey(finding)) || alertSetLine(c.currentIncidentRequest) == "" {
		t.Fatal("sdo detect does not name the joined alert")
	}
	if len(c.timeline) != before || len(w.broker.closures()) != 0 {
		t.Fatalf("timeline %d, want %d; closures %d", len(c.timeline), before, len(w.broker.closures()))
	}
}

// The alert that opened the incident fires again, twice: each time the proof
// is withdrawn, and each time it must be earned again with a full window.
func TestRecoveryCanBeWithdrawnTwice(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	for range 2 {
		w.refail()
		failed := w.controller.clock()
		w.runUntil(t, 5, nil, w.controller.IncidentOpen)
		requireOpenForWrites(t, w, gate)
		w.verify(t, gate)
		if since := w.controller.pendingClosure.VerifiedAt; since.Before(failed.Add(10*time.Second + w.controller.config.ClosureStabilityWindow)) {
			t.Fatalf("verified at %s without a fresh window", since)
		}
		for i, entry := range w.controller.pendingClosure.DetectorTimeline {
			if slices.ContainsFunc(w.controller.pendingClosure.DetectorTimeline[:i], func(prior DetectorTimelineEntry) bool { return prior.key() == entry.key() }) {
				t.Fatalf("timeline lists %q twice", entry.key())
			}
		}
	}
}

// A session that returned is retired: the fence stays and the incident
// escalates with no second dispatch.
func TestJoinAfterTheSessionReturnedKeepsTheFence(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	id := c.currentIncidentRequest.IncidentID
	c.handleDispatchCompletion(dispatchCompletion{incidentID: id, result: completedResult(id)}, c.clock())
	c.joinAlertSet(joinedCondition(), c.clock())
	c.finishEarlyReport(context.Background(), c.clock())
	if !c.IncidentOpen() || !c.responderDone || c.escalatedAt.IsZero() {
		t.Fatalf("open=%v done=%v escalated=%s", c.IncidentOpen(), c.responderDone, c.escalatedAt)
	}
	if _, err := gate.Begin(); err == nil {
		t.Fatal("a retired session may write")
	}
	if _, again := c.PendingDispatchEffect(); again {
		t.Fatal("an escalated incident dispatched again")
	}
}

// Past the response deadline the session is being retired, not resumed.
func TestJoinAtTheResponseDeadlineDoesNotResumeTheSession(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	c.joinAlertSet(joinedCondition(), c.finalResultDeadlineLocked())
	if c.IncidentOpen() {
		t.Fatal("an expired session was resumed")
	}
	if _, err := gate.Begin(); err == nil {
		t.Fatal("an expired session may write")
	}
}

// The dispatch watcher died without a result: a withdrawn recovery rejoins
// the session instead of waiting for one nobody delivers.
func TestWithdrawalRejoinsASessionWhoseWatcherFailed(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	id := c.currentIncidentRequest.IncidentID
	c.handleDispatchCompletion(dispatchCompletion{incidentID: id, err: errors.New("synthetic watch failure")}, c.clock())
	if _, early := c.PendingDispatchEffect(); early {
		t.Fatal("a verified incident dispatched")
	}
	c.joinAlertSet(joinedCondition(), c.clock())
	if _, rejoin := c.PendingDispatchEffect(); !rejoin {
		t.Fatal("the session was not rejoined")
	}
	requireWrite(t, gate)
}

// A busy gate delays the reopening by a step; a restart repeats it.
func TestTheFenceIsRemovedAfterABusyGateOrARestart(t *testing.T) {
	for _, restart := range []string{"none", "before join", "after join"} {
		t.Run(restart, func(t *testing.T) {
			w, gate := verifiedRunningSession(t)
			c := w.controller
			reload := func() {
				state, err := c.ExportState()
				if err != nil {
					t.Fatal(err)
				}
				fresh, err := NewController(c.config, []sdk.Detector{sameWriteDetector{world: w.world}}, newRestartedStore(c.clock()), w.dispatcher, c.clock())
				if err != nil {
					t.Fatal(err)
				}
				fresh.now, fresh.gitRead, fresh.broker, fresh.incidentGate = c.now, c.gitRead, w.broker, c.incidentGate
				if err := fresh.RestoreState(state); err != nil {
					t.Fatal(err)
				}
				c = fresh
			}
			if restart == "before join" {
				reload()
			}
			observation, err := gate.TryObserve()
			if err != nil || observation == nil {
				t.Fatal(err)
			}
			c.joinAlertSet(joinedCondition(), c.clock())
			if verified, _ := gate.Verified(); verified == nil || !c.IncidentOpen() {
				t.Fatal("a busy gate must leave the fence and still open the incident")
			}
			observation.Close()
			if restart == "after join" {
				reload()
			}
			c.finishEarlyReport(context.Background(), c.clock())
			requireWrite(t, gate)
			if restart != "none" && c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING {
				t.Fatalf("a restarted controller must rejoin the session: %s", c.dispatchState)
			}
		})
	}
}

// After publication froze the closure an alert belongs to the next incident.
func TestAnAlertAfterTheClosureFrozeDoesNotJoin(t *testing.T) {
	w, _ := verifiedRunningSession(t)
	c := w.controller
	c.mu.Lock()
	c.currentIncidentRequest, c.pendingClosure.Frozen = nil, true
	c.mu.Unlock()
	if c.joinAlertSet(joinedCondition(), c.clock()) || c.IncidentOpen() {
		t.Fatal("a frozen closure was withdrawn")
	}
}
