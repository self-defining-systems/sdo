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

// An exec stream the agent left open is not a write in flight: Recovered holds
// while it stays open, and the fence it publishes ends the stream.
func TestAnOpenAgentSessionDoesNotHoldRecoveredBack(t *testing.T) {
	w, gate := runningSessionWithGate(t)
	session, err := gate.BeginSession(apihistory.LeaseSubject{Verb: "exec", Object: "app/pods/web-0"})
	if err != nil {
		t.Fatal(err)
	}
	session.Established()
	t.Cleanup(func() { _ = session.Finish(false) })
	w.verify(t, gate)
	if _, err := session.Forward(func() (int, error) { return 1, nil }); err == nil {
		t.Fatal("the session still forwards after Recovered")
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
