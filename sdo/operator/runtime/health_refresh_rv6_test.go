package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func rv6Guard(c *Controller, root, compiled string) {
	c.CheckHealth = controllerHealthGuard(c, root, compiled)
}
func TestRV6PendingClosureAfterAdoption(t *testing.T) {
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
	c.pendingClosure = &IncidentClosure{Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)), FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}}, DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now}
	c.closureState = contracts.ClosureState_CLOSURE_STATE_PENDING
	b := &recordingIncidentBroker{}
	if err := c.SetIncidentBroker(b); err != nil {
		t.Fatal(err)
	}
	state := exportState(t, c)
	restored, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
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
	err = restored.ExecuteClosureEffect(ctx, effect)
	if err == nil {
		awaitBrokerResult(t, restored.closureResults)
		t.Fatalf("obsolete verified closure reached broker after adoption: %v", b.calls())
	}
	if !errors.Is(err, ErrStaleHealthInvestigation) {
		t.Fatal(err)
	}
}

func TestRV6AcknowledgmentAfterAdoption(t *testing.T) {
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
	if err := c.ExecuteClosureAcknowledgmentEffect(ctx, effect); !errors.Is(err, ErrStaleHealthInvestigation) {
		t.Fatalf("stale acknowledgment allowed: %v", err)
	}
	if len(b.calls()) != 0 {
		t.Fatalf("stale evidence reached broker: %v", b.calls())
	}
}
func TestRV6OpenIncidentAfterAdoption(t *testing.T) {
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

	visible := false
	c.OnClosureFailed = func(f ClosureFailure) { visible = f.Permanent && strings.Contains(f.LastError, "fresh investigation") }
	if err := c.Step(ctx, now.Add(time.Hour), nil); err != nil {
		t.Fatal(err)
	}
	if c.incidentOpen || !visible || c.closureState != contracts.ClosureState_CLOSURE_STATE_FAILED || len(c.failedClosures) != 1 {
		t.Fatalf("stale incident not terminal: open=%v visible=%v archive=%d", c.incidentOpen, visible, len(c.failedClosures))
	}
	if c.pendingClosure.DispatchError == "" || len(c.pendingClosure.FinalDetectorStates) != 0 {
		t.Fatal("obsolete investigation retained success")
	}
	state := exportState(t, c)
	restarted, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	rv6Guard(restarted, root, current.Commit)
	if err := restarted.Step(ctx, now.Add(2*time.Hour), nil); err != nil {
		t.Fatalf("terminal incident blocked restart: %v", err)
	}
	if restarted.incidentOpen || restarted.closureBlocksNextLocked() {
		t.Fatal("stale incident blocks fresh investigation")
	}
}
