package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type reportTailDispatcher struct {
	Dispatcher
	stopError error
}

func (d *reportTailDispatcher) StopResponder(context.Context, string, int) error {
	return d.stopError
}

// Drive the same completion and NextWake boundaries as Run, using virtual time.
// The report arrives five seconds into the unchanged thirty-second grace.
func TestControllerFinishedReportWakesBeforeGraceDeadline(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	for i := 2; i <= 17; i++ {
		step(i)
	}
	now := time.Unix(22, 0)
	c.now = func() time.Time { return now }
	c.handleDispatchCompletion(dispatchCompletion{incidentID: c.currentIncidentRequest.IncidentID, result: completedResult(c.currentIncidentRequest.IncidentID)}, now)
	// Remove incidental detector timer wakeups, which can mask the missing
	// completion wake when production detectors have long periods.
	deadlines := c.scheduler.Deadlines()
	for id := range deadlines {
		deadlines[id] = now.Add(time.Hour)
	}
	if err := c.scheduler.RestoreDeadlines(deadlines); err != nil {
		t.Fatal(err)
	}
	if wake := c.NextWake(); wake.After(now.Add(time.Second)) {
		t.Fatalf("finished responder waits until %v, want completion by %v", wake, now.Add(time.Second))
	}
	step(22)
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("finished report did not release closure/reflection")
	}
	closure, ok := c.PendingIncidentClosure()
	if !ok || !closure.ResponderCompletedAt.Equal(now) || !closure.VerifiedAt.Equal(time.Unix(17, 0)) {
		t.Fatalf("report completion changed frozen recovery timing: %+v", closure)
	}
}

func TestControllerFinishedReportStillRequiresQuiescence(t *testing.T) {
	c, d, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	c.broker = &recordingIncidentBroker{}
	for i := 2; i <= 17; i++ {
		step(i)
	}
	now := time.Unix(22, 0)
	c.now = func() time.Time { return now }
	stopper := &reportTailDispatcher{Dispatcher: d, stopError: errResponderTerminating}
	c.dispatcher = stopper
	c.handleDispatchCompletion(dispatchCompletion{incidentID: c.currentIncidentRequest.IncidentID, result: completedResult(c.currentIncidentRequest.IncidentID)}, now)
	if err := c.stepPaused(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.PendingClosureEffect(); ok {
		t.Fatal("finished dispatch bypassed worker quiescence")
	}
	if wake := c.PausedWake(); !wake.Equal(now.Add(time.Second)) {
		t.Fatalf("quiescence retry at %v, want one second", wake)
	}
	now = now.Add(time.Second)
	stopper.stopError = nil
	if err := c.stepPaused(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.PendingClosureEffect(); !ok {
		t.Fatal("quiescent worker stayed blocked until grace")
	}
}
