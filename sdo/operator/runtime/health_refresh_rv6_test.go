package runtime

import (
	"context"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func rv6Guard(c *Controller, root, compiled string) {
	c.CheckHealth = acceptedHealthGuard(root, compiled)
}

// STALE3: investigations and closures begun before a health adoption belong to
// the restarted controller. The accepted repository head moving never retires,
// refuses or fails them; closure is decided by health verification alone.
func TestPendingClosureSurvivesAdoption(t *testing.T) {
	ctx := context.Background()
	root, _, _, _, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingSwap(t, root)
	current := tracingRefresh(t, root, coordinator)
	now := time.Unix(0, 0)
	cfg := testControllerConfig()
	cfg.SourceCommit = current.Commit
	detectors := []sdk.Detector{controllerDetector("health", time.Second, sdk.Finding{})}
	c, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
	request.SourceCommit = old.Commit
	request.DetectorHistory = testDetectorHistory(now)
	c.pendingClosure = &IncidentClosure{Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)), FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}}, DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now}
	c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	b := &recordingIncidentBroker{}
	if err := c.SetIncidentBroker(b); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, c)
	restored, err := NewController(cfg, detectors, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	if err := restored.SetIncidentBroker(b); err != nil {
		t.Fatal(err)
	}
	restored.discardRestoredHealthObservations(now)
	rv6Guard(restored, root, current.Commit)
	effect, ok := restored.PendingClosureEffect()
	if !ok {
		t.Fatal("missing closure")
	}
	if err := restored.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatalf("adopted controller refused a closure begun before adoption: %v", err)
	}
	awaitBrokerResult(t, restored.closureResults)
	if len(b.calls()) == 0 {
		t.Fatal("closure begun before adoption never reached the broker")
	}
	if restored.closureState == contracts.ClosureState_CLOSURE_STATE_FAILED || len(restored.failedClosures) != 0 {
		t.Fatalf("closure retired by adoption: state %v archive %d", restored.closureState, len(restored.failedClosures))
	}
}

func TestAcknowledgmentSurvivesAdoption(t *testing.T) {
	ctx := context.Background()
	root, _, _, _, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingSwap(t, root)
	current := tracingRefresh(t, root, coordinator)
	now := time.Unix(0, 0)
	c, err := NewController(testControllerConfig(), nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
	request.SourceCommit = old.Commit
	c.pendingClosure = &IncidentClosure{Request: request}
	c.closureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
	c.closureReceipt = &ClosureReceipt{IncidentID: request.IncidentID, OutcomeCommit: current.Commit, AckToken: "token"}
	b := &recordingIncidentBroker{}
	if err := c.SetIncidentBroker(b); err != nil {
		t.Fatal(err)
	}
	rv6Guard(c, root, current.Commit)
	effect, ok := c.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("missing acknowledgment")
	}
	if err := c.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
		t.Fatalf("adoption refused the acknowledgment: %v", err)
	}
}

func TestOpenIncidentSurvivesAdoption(t *testing.T) {
	ctx := context.Background()
	root, _, _, _, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingSwap(t, root)
	current := tracingRefresh(t, root, coordinator)
	now := time.Unix(0, 0)
	cfg := testControllerConfig()
	cfg.SourceCommit = current.Commit
	c, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	request := c.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
	request.SourceCommit = old.Commit
	request.DetectorHistory = testDetectorHistory(now)
	c.currentIncidentRequest = &request
	c.incidentOpen = true
	c.responderDone = true
	c.responderCompletedAt = now
	rv6Guard(c, root, current.Commit)
	failed := false
	c.OnClosureFailed = func(ClosureFailure) { failed = true }
	if err := c.Step(ctx, now.Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if !c.incidentOpen && c.closureState == contracts.ClosureState_CLOSURE_STATE_FAILED || failed || len(c.failedClosures) != 0 {
		t.Fatalf("adoption retired an investigation: open=%v failed=%v archive=%d", c.incidentOpen, failed, len(c.failedClosures))
	}
}
