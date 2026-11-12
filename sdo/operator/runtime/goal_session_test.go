package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// resumingDispatcher offers the old re-prompt hook. The controller must never
// call it: SDO sends the agent one task per incident.
type resumingDispatcher struct {
	recordingDispatcher
	resumes atomic.Int32
}

func (d *resumingDispatcher) ContinueAfterVerification(context.Context, IncidentRequest, ToolResponse, time.Time) (bool, error) {
	d.resumes.Add(1)
	return true, nil
}

// goalSessionController is an open incident whose responder returned a while
// ago. The opening finding is active when unhealthy is set.
func goalSessionController(t *testing.T, unhealthy bool, dispatcher Dispatcher) (*Controller, *time.Time, *[]IncidentClosure) {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	finding := sdk.Finding{}
	if unhealthy {
		finding = sdk.Finding{RuleID: "unhealthy", Status: sdk.FindingActive}
	}
	d := controllerDetector("health", time.Second, finding)
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	d.spec.OriginatingCommit = "synthetic"
	d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = time.Second
	c, err := NewController(cfg, []sdk.Detector{d}, staticProvider{}, dispatcher, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	c.incidentOpen, c.responderDone = true, true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", ResponseDeadline: now.Add(time.Hour)}
	c.incidentDetectedAt, c.incidentDispatchedAt = now.Add(-time.Hour), now.Add(-time.Hour)
	// The responder returned longer ago than the grace in which Recovered may still hold.
	c.responderCompletedAt = now.Add(-cfg.VerificationTimeout - time.Minute)
	result := completedResult("synthetic")
	c.currentIncidentResult = &result
	closures := &[]IncidentClosure{}
	c.OnIncidentClosed = func(closure IncidentClosure) { *closures = append(*closures, closure) }
	return c, &now, closures
}

func stepGoalSession(t *testing.T, c *Controller, now *time.Time, steps int) {
	t.Helper()
	for i := 0; i < steps; i++ {
		if err := c.Step(context.Background(), *now, nil); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(time.Second)
	}
}

func TestReturnBeforeRecoveredEscalatesWithoutAnotherAgentTurn(t *testing.T) {
	dispatcher := &resumingDispatcher{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 8)}}
	c, now, closures := goalSessionController(t, true, dispatcher)
	stepGoalSession(t, c, now, 6)
	if got := dispatcher.resumes.Load(); got != 0 {
		t.Fatalf("the responder was re-prompted %d times for the same symptoms", got)
	}
	if got := dispatcher.callCount(); got != 0 {
		t.Fatalf("a new agent was dispatched %d times for the same symptoms", got)
	}
	if len(*closures) != 0 {
		t.Fatal("an escalated incident closed while its opening alert was still active")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.escalatedAt.IsZero() || !c.incidentOpen {
		t.Fatalf("incident must be escalated and open: escalatedAt=%v open=%v", c.escalatedAt, c.incidentOpen)
	}
	if until := c.busyUntilLocked(*now); !until.IsZero() {
		t.Fatalf("an escalated incident published a busy bound %v; a harness must read it as the fault's end state", until)
	}
}

func TestEscalatedIncidentClosesEscalatedWhenItsAlertsClear(t *testing.T) {
	c, now, closures := goalSessionController(t, false, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	c.escalatedAt = now.Add(-time.Minute)
	stepGoalSession(t, c, now, 4)
	if len(*closures) != 1 {
		t.Fatalf("closures = %d, want 1", len(*closures))
	}
	if (*closures)[0].EscalatedAt.IsZero() {
		t.Fatal("the closure lost escalated_at")
	}
}

func TestRecoveredInsideTheGraceAfterAReturnIsNotAnEscalation(t *testing.T) {
	c, now, closures := goalSessionController(t, false, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	c.responderCompletedAt = now.Add(-time.Second)
	stepGoalSession(t, c, now, 4)
	if len(*closures) != 1 || !(*closures)[0].EscalatedAt.IsZero() {
		t.Fatalf("a fix that held inside the grace was labelled escalated: %+v", *closures)
	}
}

func TestResponseTimeoutIsAnEscalationNotAFailure(t *testing.T) {
	c, now, _ := goalSessionController(t, true, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	c.responderDone, c.responderCompletedAt = false, time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	cause := &ResponderJobFailedError{JobName: "job", Cause: errors.Join(&ResponseTimeoutError{Deadline: *now}, context.Canceled)}
	c.handleDispatchCompletion(dispatchCompletion{incidentID: "synthetic", err: cause}, *now)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.escalatedAt.IsZero() {
		t.Fatal("the response timeout did not escalate")
	}
	if c.dispatchError != "" {
		t.Fatalf("the response timeout was recorded as a dispatch failure: %q", c.dispatchError)
	}
	if !c.responderDone {
		t.Fatal("the responder is not done after its timeout")
	}
}

func TestClosedIncidentCarriesTheSessionTheClosingTurnResumes(t *testing.T) {
	loop := &ResponderLoop{menuSession: &contracts.MitigationSession{ProviderSessionId: "session-1"}}
	result := earlyResultSnapshot(loop, IncidentRequest{IncidentID: "synthetic"}, time.Unix(1000, 0).UTC())
	if result.ResponderSessionID != "session-1" {
		t.Fatalf("the controller-built result lost the session id: %q", result.ResponderSessionID)
	}
}
