package runtime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
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

// joinedUserTest is a user test that starts failing: only user tests and
// failing user traffic join an alert set.
func joinedUserTest() sdk.Finding {
	finding := sdk.Finding{DetectorID: memory.TrafficDetectorPrefix + "other", RuleID: traffic.RuleIDPrefix + "checkout", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "other"}, Summary: "checkout fails", Evidence: "synthetic"}
	finding.Fingerprint = FindingFingerprint(finding)
	return finding
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
	finding := joinedUserTest()
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

// A session that returned is retired. A user test that starts failing after
// it returned is a symptom nobody works on: it does not join, the incident
// closes on its own alert set with the fence standing, and the test waits for
// the next incident.
func TestAUserTestAfterTheSessionReturnedWaitsForTheNextIncident(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	id := c.currentIncidentRequest.IncidentID
	c.handleDispatchCompletion(dispatchCompletion{incidentID: id, result: completedResult(id)}, c.clock())
	if c.joinAlertSet(joinedUserTest(), c.clock()) {
		t.Fatal("a user test joined after the session returned")
	}
	c.finishEarlyReport(context.Background(), c.clock())
	if c.IncidentOpen() || !c.escalatedAt.IsZero() || c.pendingClosure == nil || c.pendingClosure.Request.IncidentID != id ||
		len(c.pendingClosure.Request.JoinedKeys) != 0 || !c.pendingClosure.EscalatedAt.IsZero() {
		t.Fatalf("open=%v escalated=%s pending=%+v", c.IncidentOpen(), c.escalatedAt, c.pendingClosure)
	}
	if _, err := gate.Begin(); err == nil {
		t.Fatal("a retired session may write")
	}
	if _, again := c.PendingDispatchEffect(); again {
		t.Fatal("the closing incident dispatched again")
	}
}

// A session that is being retired, inside the join window, is not resumed.
func TestJoinWhileTheSessionIsRetiredDoesNotResumeIt(t *testing.T) {
	for name, retire := range map[string]func(*Controller){
		"response deadline passed": func(c *Controller) { c.currentIncidentRequest.ResponseDeadline = c.pendingClosure.VerifiedAt },
		"stop being retried":       func(c *Controller) { c.earlyStopRetryAt = c.clock().Add(time.Second) },
		"watch only":               func(c *Controller) { c.config.WatchOnly = true },
	} {
		t.Run(name, func(t *testing.T) {
			w, gate := verifiedRunningSession(t)
			c := w.controller
			retire(c)
			finding := joinedUserTest()
			if !c.joinAlertSet(finding, c.clock()) || !slices.Contains(c.currentIncidentRequest.JoinedKeys, findingKey(finding)) {
				t.Fatal("the alert did not join")
			}
			if c.IncidentOpen() {
				t.Fatal("a session being retired was resumed")
			}
			if _, err := gate.Begin(); err == nil {
				t.Fatal("a session being retired may write")
			}
		})
	}
}

// An alert of the set that fires again restarts the proof.
func TestAMemberThatFiresAgainRestartsTheStabilityProof(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	c.recoverySafetyLost = false
	if c.closureStableSince.IsZero() {
		t.Fatal("fixture has no running proof")
	}
	c.joinAlertSet(c.currentIncidentRequest.Findings[0], c.clock())
	requireOpenForWrites(t, w, gate)
	if !c.closureStableSince.IsZero() || !c.recoverySafetyLost {
		t.Fatalf("the old proof survived: stable=%s lost=%v", c.closureStableSince, c.recoverySafetyLost)
	}
}

// A restart forgets whether the fence stands, never that the responder returned.
func TestARestartKeepsAReturnedRespondersFence(t *testing.T) {
	w, gate := verifiedRunningSession(t)
	c := w.controller
	id := c.currentIncidentRequest.IncidentID
	c.handleDispatchCompletion(dispatchCompletion{incidentID: id, result: completedResult(id)}, c.clock())
	c.finishEarlyReport(context.Background(), c.clock())
	// The closure recheck saw an alert of the set fire again: the incident
	// returns to observation with its responder retired.
	c.mu.Lock()
	if err := c.reobserveClosureLocked(c.clock(), &c.pendingClosure.Request); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	c.mu.Unlock()
	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	c.finishEarlyReport(context.Background(), c.clock())
	if !c.IncidentOpen() || !c.responderDone {
		t.Fatal("fixture lost the escalated incident")
	}
	if _, err := gate.Begin(); err == nil {
		t.Fatal("a restart let a returned responder write")
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
	c.joinAlertSet(joinedUserTest(), c.clock())
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
			c.joinAlertSet(joinedUserTest(), c.clock())
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
	if c.joinAlertSet(joinedUserTest(), c.clock()) || c.IncidentOpen() {
		t.Fatal("a frozen closure was withdrawn")
	}
}

// Every command answers through the loop, whatever it does: it carries the
// controller's verdict of now, never the fence file's.
func TestCommandsTellAWithdrawnRecovery(t *testing.T) {
	commands := []string{"source", "detect", "changes", "timeline", "map", "menu", "investigations", "mitigate", "rollback"}
	w, gate := verifiedRunningSession(t)
	c := w.controller
	id := c.currentIncidentRequest.IncidentID
	loop := &ResponderLoop{Request: *cloneIncidentRequest(c.currentIncidentRequest), Provider: c.provider, MutationGate: gate,
		IncidentStatus: func() *apihistory.IncidentVerified { return c.incidentVerificationWarning(id) }}
	all := func(check func(string, ToolResponse)) {
		t.Helper()
		for _, command := range commands {
			check(command, loop.Handle(context.Background(), ToolRequest{Command: command}))
		}
	}
	all(func(command string, response ToolResponse) {
		if response.RecoveryWithdrawn != "" {
			t.Errorf("%s: withdrawn before it was: %q", command, response.RecoveryWithdrawn)
		}
		if command == "investigations" {
			return
		}
		// A test can fail or lack samples before it joins, so the notice never
		// invites the agent to stop on SDO's verdict alone.
		if response.IncidentVerified == nil || strings.Contains(response.IncidentVerified.Message, "finish") ||
			!strings.Contains(response.IncidentVerified.Message, "return unless a test of this incident fails") {
			t.Errorf("%s: %+v", command, response.IncidentVerified)
		}
	})
	// The gate is busy, so the fence file outlives the withdrawal for a step.
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatal(err)
	}
	c.joinAlertSet(joinedUserTest(), c.clock())
	told := func(command string, response ToolResponse) {
		if response.IncidentVerified != nil || !strings.Contains(response.RecoveryWithdrawn, "recovery withdrawn: checkout fires") {
			t.Errorf("%s: verified %v withdrawn %q", command, response.IncidentVerified, response.RecoveryWithdrawn)
		}
	}
	for _, command := range []string{"source", "detect", "changes", "investigations"} {
		told(command, loop.Handle(context.Background(), ToolRequest{Command: command}))
	}
	observation.Close()
	c.tracker.Restore(map[string]FindingState{findingKey(joinedUserTest()): {DetectorID: joinedUserTest().DetectorID, Finding: joinedUserTest(), Active: true}})
	c.finishEarlyReport(context.Background(), c.clock())
	all(told)
	// A new verification replaces the notice.
	observation, err = gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatal(err)
	}
	if err := observation.Fence(c.clock()); err != nil {
		t.Fatal(err)
	}
	observation.Close()
	if reason, _ := gate.Withdrawn(); reason != "" {
		t.Fatal("a new verification kept the old notice")
	}
}

func TestAWithdrawnAlertIsNamedInOneShortLine(t *testing.T) {
	for want, finding := range map[string]sdk.Finding{
		"write-item":         {RuleID: "scenario-slo.write-item", PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"}},
		"rule-x Service/api": {RuleID: "rule-x", PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "api"}},
	} {
		if got := alertName(finding); got != want {
			t.Errorf("%q, want %q", got, want)
		}
	}
	if long := alertName(sdk.Finding{RuleID: strings.Repeat("x", 200)}); len([]rune(long)) != 50 {
		t.Errorf("unbounded name: %q", long)
	}
}

// The notice names an alert that fires now: it ends when that alert clears,
// also across a restart.
func TestTheWithdrawnNoticeEndsWhenItsAlertClears(t *testing.T) {
	for _, restart := range []bool{false, true} {
		w, gate := verifiedRunningSession(t)
		c := w.controller
		w.refail()
		w.runUntil(t, 5, nil, c.IncidentOpen)
		w.runUntil(t, 2, nil, func() bool { return false })
		if reason, _ := gate.Withdrawn(); !strings.Contains(reason, "request-stall Deployment/api fires") {
			t.Fatalf("the firing alert is not named: %q", reason)
		}
		if restart {
			state, err := c.ExportState()
			if err != nil {
				t.Fatal(err)
			}
			c.withdrawnNoticeMayStand = false
			if err := c.RestoreState(state); err != nil {
				t.Fatal(err)
			}
			c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
		}
		w.runUntil(t, 12, nil, func() bool { return false })
		if reason, _ := gate.Withdrawn(); reason != "" || !c.IncidentOpen() {
			t.Fatalf("restart %v: a cleared alert is said to fire: %q", restart, reason)
		}
	}
}
