package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type stopCountingDispatcher struct {
	recordingDispatcher
	smu   sync.Mutex
	stops int
}

func (d *stopCountingDispatcher) StopResponder(context.Context, string, int) error {
	d.smu.Lock()
	defer d.smu.Unlock()
	d.stops++
	return nil
}

func (d *stopCountingDispatcher) stopCount() int {
	d.smu.Lock()
	defer d.smu.Unlock()
	return d.stops
}

func earlyClosureController(t *testing.T, now time.Time) (*Controller, *stopCountingDispatcher) {
	t.Helper()
	d := &stopCountingDispatcher{}
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, d, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	request := IncidentRequest{IncidentID: "synthetic", Application: "demo", ResponseDeadline: now.Add(time.Minute)}
	c.currentIncidentRequest = &request
	result := completedResult("synthetic")
	c.pendingClosure = &IncidentClosure{Request: request, Result: &result, VerifiedAt: now, VerifiedWhileResponderRunning: true}
	return c, d
}

// Recovery that verifies while the responder session is still running waits
// for that session's final result: an earlier structured result is
// provisional, and the changes made after it are in the final one. The wait is
// bounded by the response deadline.
func TestEarlyClosureWaitsForTheSessionsFinalResult(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	c, d := earlyClosureController(t, now)

	c.finishEarlyReport(ctx, now.Add(10*time.Second))
	if d.stopCount() != 0 || c.currentIncidentRequest == nil {
		t.Fatalf("the running responder was retired before its final result (stops %d)", d.stopCount())
	}
	if wake := c.earlyReportWakeLocked(); wake.IsZero() || wake.After(c.currentIncidentRequest.ResponseDeadline) {
		t.Fatalf("the wait is not bounded by the response deadline: %s", wake)
	}

	// The final result arrives with a change made after the early one.
	final := completedResult("synthetic")
	final.AgentChanges = []*contracts.AgentChange{{Summary: "reset the stuck package rows"}}
	c.handleDispatchCompletion(dispatchCompletion{incidentID: "synthetic", result: final}, now.Add(20*time.Second))
	c.finishEarlyReport(ctx, now.Add(21*time.Second))
	if d.stopCount() != 1 || c.currentIncidentRequest != nil {
		t.Fatalf("the finished session was not retired (stops %d)", d.stopCount())
	}
	if got := c.pendingClosure.Result.AgentChanges; len(got) != 1 {
		t.Fatalf("the final result's agent changes did not reach the closure: %v", got)
	}
}

func TestEarlyClosureStopsAtTheResponseDeadline(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	c, d := earlyClosureController(t, now)
	c.finishEarlyReport(ctx, now.Add(2*time.Minute))
	if d.stopCount() != 1 || c.currentIncidentRequest != nil {
		t.Fatalf("the stalled responder was not retired at the deadline (stops %d)", d.stopCount())
	}
}
