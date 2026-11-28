package runtime

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// resumingDispatcher offers the old re-prompt hook. The controller must never
// call it: a resume runs in the session's own worker (answerFinish).
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
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: []sdk.Finding{{DetectorID: "health", RuleID: "unhealthy", Status: sdk.FindingActive}}, ResponseDeadline: now.Add(time.Hour)}
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

// A session that returned before Recovered, with its alert still failing, is
// never escalated: once SDO's writes settled, a fresh paced session starts on
// the same incident with a Go-rendered summary of the one before it.
func TestReturnBeforeRecoveredStartsAFreshSessionNeverAnEscalation(t *testing.T) {
	dispatcher := &resumingDispatcher{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 8)}}
	c, now, closures := goalSessionController(t, true, dispatcher)
	stepGoalSession(t, c, now, 6)
	if got := dispatcher.resumes.Load(); got != 0 {
		t.Fatalf("the old re-prompt hook ran %d times", got)
	}
	if len(*closures) != 0 {
		t.Fatal("the incident closed while its opening alert was still active")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	request := c.currentIncidentRequest
	if !c.incidentOpen || request.SessionGeneration != 1 || c.responderDone || request.PreparedAgentTask != "" {
		t.Fatalf("want an open incident with a fresh session pending: open=%v generation=%d done=%v", c.incidentOpen, request.SessionGeneration, c.responderDone)
	}
	if !strings.Contains(request.PriorSession, "earlier agent session") || !strings.Contains(request.PriorSession, "unhealthy") {
		t.Fatalf("prior session summary = %q", request.PriorSession)
	}
	if !request.ResponseDeadline.After(*now) {
		t.Fatalf("the fresh session has no response window of its own: %s", request.ResponseDeadline)
	}
	if until := c.busyUntilLocked(*now); until.IsZero() {
		t.Fatal("an incident SDO still works published no busy bound")
	}
}

func TestRecoveredInsideTheGraceAfterAReturnClosesVerified(t *testing.T) {
	c, now, closures := goalSessionController(t, false, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	c.responderCompletedAt = now.Add(-time.Second)
	stepGoalSession(t, c, now, 4)
	if len(*closures) != 1 || (*closures)[0].HealthClearedAt == nil || (*closures)[0].Request.SessionGeneration != 0 {
		t.Fatalf("a fix that held inside the grace did not close verified on its first session: %+v", *closures)
	}
}

// The response timeout ends the session, never the incident: no dispatch
// failure is recorded, and the next pass starts a fresh session.
func TestResponseTimeoutEndsTheSessionNotTheIncident(t *testing.T) {
	c, now, _ := goalSessionController(t, true, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	c.responderDone, c.responderCompletedAt = false, time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	cause := &ResponderJobFailedError{JobName: "job", Cause: errors.Join(&ResponseTimeoutError{Deadline: *now}, context.Canceled)}
	c.handleDispatchCompletion(dispatchCompletion{incidentID: "synthetic", err: cause}, *now)
	c.mu.Lock()
	if c.dispatchError != "" {
		t.Fatalf("the response timeout was recorded as a dispatch failure: %q", c.dispatchError)
	}
	if !c.responderDone {
		t.Fatal("the responder is not done after its timeout")
	}
	c.config.VerificationTimeout = time.Second
	c.mu.Unlock()
	stepGoalSession(t, c, now, 4)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.incidentOpen || c.currentIncidentRequest.SessionGeneration != 1 {
		t.Fatalf("after the timeout: open=%v generation=%d", c.incidentOpen, c.currentIncidentRequest.SessionGeneration)
	}
}

func TestClosedIncidentCarriesTheSessionTheClosingTurnResumes(t *testing.T) {
	loop := &ResponderLoop{menuSession: &contracts.MitigationSession{ProviderSessionId: "session-1"}}
	result := earlyResultSnapshot(loop, IncidentRequest{IncidentID: "synthetic"}, time.Unix(1000, 0).UTC())
	if result.ResponderSessionID != "session-1" {
		t.Fatalf("the controller-built result lost the session id: %q", result.ResponderSessionID)
	}
}

// Past its grace, an alert-opened incident whose alerts are not failing waits
// for the next observation; the expired grace is never a wake (it would spin
// the controller loop).
func TestAnExpiredGraceWithNothingFailingIsNotAWake(t *testing.T) {
	c, now, _ := goalSessionController(t, false, &recordingDispatcher{requests: make(chan IncidentRequest, 1)})
	if wake, reason := c.nextWake(); reason == "verification deadline" {
		t.Fatalf("an expired grace is the next wake at %s (now %s)", wake, *now)
	}
}
