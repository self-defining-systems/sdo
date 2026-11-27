package runtime

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

// runningSessionWithGate dispatches the responder and gives it a mutation gate.
func runningSessionWithGate(t *testing.T) (*closureRestartWorld, *apihistory.MutationGate) {
	t.Helper()
	w := newClosureRestartWorld(t)
	gate, err := apihistory.PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c := w.controller
	gate.Clock = c.clock
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
	return w, gate
}

// openSession is an exec stream the agent opened and left open.
func openSession(t *testing.T, gate *apihistory.MutationGate) *apihistory.SessionLease {
	t.Helper()
	session, err := gate.BeginSession(apihistory.LeaseSubject{Verb: "exec", Object: "app/pods/web-0"})
	if err != nil {
		t.Fatal(err)
	}
	session.Established()
	t.Cleanup(func() { _ = session.Finish(false) })
	return session
}

func requireSessionNotEnded(t *testing.T, session *apihistory.SessionLease) {
	t.Helper()
	select {
	case <-session.Ended():
		t.Fatal("a provisional recovery ended the agent's session")
	default:
	}
}

// An exec stream the agent left idle is not a write in flight: Recovered holds
// while it stays open. The fence is provisional, so it does not end the session.
func TestAnIdleAgentSessionDoesNotHoldRecoveredBack(t *testing.T) {
	w, gate := runningSessionWithGate(t)
	session := openSession(t, gate)
	w.verify(t, gate)
	requireSessionNotEnded(t, session)
}

// A session in use (a backfill printing progress, a psql shell being typed in)
// counts as a write while bytes flow: Recovered holds one stability window
// after its last bytes, and the agent is told so.
func TestAnActiveAgentSessionKeepsRecoveredFromHolding(t *testing.T) {
	w, gate := runningSessionWithGate(t)
	c := w.controller
	session := openSession(t, gate)
	window := c.config.ClosureStabilityWindow
	activeUntil := w.healedAt.Add(3 * window)
	var lastBytes time.Time
	var line string
	w.runUntil(t, 200, nil, func() bool {
		if now := c.clock(); now.Before(activeUntil) {
			session.Touch()
			lastBytes = now
		}
		if l := c.recoveryWaitingLine(c.currentIncidentRequest.IncidentID); strings.Contains(l, "your open session") {
			line = l
		}
		return c.earlyReportPendingLocked()
	})
	if !c.earlyReportPendingLocked() {
		t.Fatal("Recovered never held after the session went idle")
	}
	if verifiedAt := c.pendingClosure.VerifiedAt; verifiedAt.Before(lastBytes.Add(window)) {
		t.Fatalf("verified at %s, %s after the session's last bytes at %s", verifiedAt, verifiedAt.Sub(lastBytes), lastBytes)
	}
	if !strings.Contains(line, "exec app/pods/web-0, last bytes at ") {
		t.Fatalf("the agent was not told its session holds Recovered: %q", line)
	}
	requireSessionNotEnded(t, session)
}

// A withdrawn recovery leaves the agent's open session working: the bytes it
// sends after the withdrawal go through.
func TestAWithdrawnRecoveryKeepsTheAgentSession(t *testing.T) {
	w, gate := runningSessionWithGate(t)
	session := openSession(t, gate)
	w.verify(t, gate)
	w.refail()
	w.runUntil(t, 5, nil, w.controller.IncidentOpen)
	requireOpenForWrites(t, w, gate)
	requireSessionNotEnded(t, session)
	if _, err := session.Forward(nil, func() (int, error) { return 1, nil }); err != nil {
		t.Fatalf("the session may not write after a withdrawn recovery: %v", err)
	}
}

// A write that never finishes keeps Recovered from holding, and the agent is
// told which write and since when.
func TestRecoveredNamesTheWriteItWaitsOn(t *testing.T) {
	w, gate := runningSessionWithGate(t)
	lease, err := gate.BeginWrite(apihistory.LeaseSubject{Verb: "patch", Object: "app/deployments/web"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Finish(false) })
	c := w.controller
	w.runUntil(t, 60, nil, func() bool { return c.earlyReportPendingLocked() })
	if c.earlyReportPendingLocked() {
		t.Fatal("recovery was verified while a write was in flight")
	}
	id := c.currentIncidentRequest.IncidentID
	if line := c.recoveryWaitingLine(id); !strings.Contains(line, "write in flight: patch app/deployments/web since ") {
		t.Fatalf("waiting line %q", line)
	}
	if summary := c.cachedFindingView(id).Summary; !strings.Contains(summary, "patch app/deployments/web") {
		t.Fatalf("sdo detect --wait summary %q", summary)
	}
}

// While only time is left, the agent is told how much.
func TestRecoveredNamesTheStabilityWindowLeft(t *testing.T) {
	w, _ := runningSessionWithGate(t)
	c := w.controller
	var line string
	w.runUntil(t, 60, nil, func() bool {
		line = c.recoveryWaitingLine(c.currentIncidentRequest.IncidentID)
		return strings.Contains(line, "stability window") || c.earlyReportPendingLocked()
	})
	if !strings.Contains(line, "stability window: ") || !strings.Contains(line, " s left") {
		t.Fatalf("waiting line %q", line)
	}
}
