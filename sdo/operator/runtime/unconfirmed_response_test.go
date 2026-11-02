package runtime

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/sdktest"
)

func TestUnconfirmedVerificationClosesWithReceiptAtBound(t *testing.T) {
	for _, status := range []DetectorEvaluationStatus{DetectorEvaluationPending, DetectorEvaluationError} {
		for _, unavailable := range []bool{false, true} {
			for _, responseBudget := range []time.Duration{10 * time.Second, time.Hour} {
				t.Run(fmt.Sprintf("%s/snapshot-unavailable=%v/response-budget=%s", status, unavailable, responseBudget), func(t *testing.T) {
					now := time.Unix(1000, 0).UTC()
					cfg := testControllerConfig()
					cfg.VerificationTimeout = 30 * time.Second
					snapshot := sdktest.Snapshot{}
					c, err := NewController(cfg, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, now)
					if err != nil {
						t.Fatal(err)
					}
					broker := &recordingClosureBroker{}
					if err := c.SetIncidentBroker(broker); err != nil {
						t.Fatal(err)
					}
					if unavailable {
						c.provider = failingSnapshotProvider{}
					}
					c.now = func() time.Time { return now }
					request := goldenRequest(t)
					request.ResponseDeadline = now.Add(responseBudget)
					c.currentIncidentRequest = &request
					result := completedResult(request.IncidentID)
					c.currentIncidentResult = &result
					c.incidentOpen, c.responderDone = true, true
					c.responderCompletedAt = now
					c.healthDetectorIDs = []string{"health"}
					c.history = []DetectorEvaluation{{DetectorID: "health", Status: status, Reason: "window incomplete", Error: "observer unavailable", EvaluatedAt: now}}
					c.maybeCloseIncident(context.Background(), now, nil)
					if c.pendingClosure != nil {
						t.Fatal("closed before verification bound")
					}
					now = now.Add(min(cfg.VerificationTimeout, responseBudget))
					c.maybeCloseIncident(context.Background(), now, nil)
					if c.pendingClosure == nil || c.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || !strings.Contains(c.pendingClosure.UnconfirmedDetail, "health") {
						t.Fatalf("missing typed unconfirmed closure: %+v", c.pendingClosure)
					}
					executeRecheck(t, c)
					awaitBrokerResult(t, c.closureResults)
					c.processBrokerCompletions()
					if c.closureReceipt == nil || c.closureReceipt.OutcomeCommit == "" || c.closureFailure != nil {
						t.Fatalf("closure has no receipt or retired OwnerLost: receipt=%+v failure=%+v", c.closureReceipt, c.closureFailure)
					}
				})
			}
		}
	}
}

func TestIncidentDetectorRequiresClearEvaluationForHealthyClosure(t *testing.T) {
	for _, status := range []DetectorEvaluationStatus{DetectorEvaluationPending, DetectorEvaluationError, DetectorEvaluationFiring, DetectorEvaluationClear} {
		t.Run(string(status), func(t *testing.T) {
			now := time.Unix(1000, 0)
			c, err := NewController(testControllerConfig(), nil, staticProvider{snapshot: sdktest.Snapshot{}}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := goldenRequest(t)
			c.currentIncidentRequest = &request
			c.history = []DetectorEvaluation{{DetectorID: request.Findings[0].DetectorID, Status: status, Reason: "window incomplete", EvaluatedAt: now}}
			if states, healthy := c.finalVerificationStates(); healthy != (status == DetectorEvaluationClear) {
				t.Fatalf("non-clear incident detector supplied health authority: %+v", states)
			}
		})
	}
}

func TestVerificationBoundSurvivesContinuationAndControllerRestart(t *testing.T) {
	now := time.Unix(1000, 0)
	cfg := testControllerConfig()
	cfg.VerificationTimeout = 30 * time.Second
	snapshot := sdktest.Snapshot{}
	d := &verificationRetryDispatcher{}
	c, err := NewController(cfg, nil, staticProvider{snapshot: snapshot}, d, now)
	if err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.ResponseDeadline = now.Add(time.Hour)
	c.currentIncidentRequest = &request
	c.incidentOpen, c.responderDone = true, true
	c.responderCompletedAt = now
	result := completedResult(request.IncidentID)
	c.currentIncidentResult = &result
	c.healthDetectorIDs = []string{"health"}
	c.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationPending, Reason: "window incomplete", EvaluatedAt: now}}
	c.maybeCloseIncident(context.Background(), now, snapshot)
	if d.resumed != 1 || c.responderDone {
		t.Fatal("first verification did not resume")
	}
	state := exportState(t, c)
	restored, err := NewController(cfg, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	restored.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: result}, now.Add(20*time.Second))
	if got := restored.verificationDeadlineLocked(); !got.Equal(now.Add(cfg.VerificationTimeout)) {
		t.Fatalf("continuation/restart extended verification: got %v want %v", got, now.Add(cfg.VerificationTimeout))
	}
	restored.maybeCloseIncident(context.Background(), now.Add(cfg.VerificationTimeout), snapshot)
	if restored.pendingClosure == nil || restored.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatal("original verification bound did not close unconfirmed")
	}
}

func TestExpiredVerificationDoesNotDispatchPendingContinuation(t *testing.T) {
	now := time.Unix(1000, 0)
	cfg := testControllerConfig()
	cfg.VerificationTimeout = 30 * time.Second
	d := &verificationRetryDispatcher{}
	c, err := NewController(cfg, nil, staticProvider{snapshot: sdktest.Snapshot{}}, d, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	request := goldenRequest(t)
	request.ResponseDeadline = now.Add(time.Hour)
	c.currentIncidentRequest = &request
	c.incidentOpen, c.responderDone = true, true
	c.responderCompletedAt = now
	result := completedResult(request.IncidentID)
	c.currentIncidentResult = &result
	c.healthDetectorIDs = []string{"health"}
	c.history = []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationPending, Reason: "window incomplete", EvaluatedAt: now}}
	c.maybeCloseIncident(context.Background(), now, sdktest.Snapshot{})
	effect, ok := c.PendingDispatchEffect()
	if !ok || d.resumed != 1 {
		t.Fatal("continuation was not pending")
	}
	state := exportState(t, c)
	if err := c.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	now = now.Add(cfg.VerificationTimeout)
	if _, ok := c.PendingDispatchEffect(); ok {
		t.Fatal("expired continuation can launch")
	}
	if err := c.ExecuteDispatchEffect(context.Background(), effect); err == nil {
		t.Fatal("stale effect launched expired continuation")
	}
	if err := c.stepPaused(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if c.pendingClosure == nil || c.closureFailure != nil || c.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatal("pending continuation did not close unconfirmed")
	}
}
