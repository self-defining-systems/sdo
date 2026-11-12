package runtime

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// blockingDispatcher is a responder that is still working when the incident
// heals by itself, and returns only when its context ends.
type blockingDispatcher struct {
	mu      sync.Mutex
	started bool
	// stopAfter is when the responder's Job finally terminates; before it,
	// StopResponder reports the Job still terminating.
	stopAfter time.Time
	clock     func() time.Time
}

func (d *blockingDispatcher) hasStarted() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.started
}

func (d *blockingDispatcher) StopResponder(context.Context, string, int) error {
	if d.clock != nil && d.clock().Before(d.stopAfter) {
		return errResponderTerminating
	}
	return nil
}

func (d *blockingDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	d.started = true
	d.mu.Unlock()
	<-ctx.Done()
	return IncidentResult{}, ctx.Err()
}

// A workload that heals by itself while the responder is still investigating
// (no repair, every health detector clear) closes promptly as verified, not
// after the responder or the verification timeout.
func TestSelfHealedIncidentClosesPromptlyWhileResponderRuns(t *testing.T) {
	selfHealedIncident(t, 0)
}

// The responder's Job can take minutes to terminate. The verification deadline
// belongs to a window the controller can still complete, so a healed incident
// whose responder was slow to stop still closes verified, not unconfirmed.
func TestSelfHealedIncidentSurvivesSlowResponderTermination(t *testing.T) {
	selfHealedIncident(t, 9*time.Minute)
}

func selfHealedIncident(t *testing.T, terminating time.Duration) {
	t.Helper()
	ctx := context.Background()
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	store := newRestartedStore(opened)
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	dispatcher := &blockingDispatcher{stopAfter: opened.Add(20*time.Second + terminating)}
	slow := controllerDetector("slow-probe", 10*time.Minute, sdk.Finding{})
	slow.spec.Class, slow.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	slow.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	slow.spec.OriginatingCommit = "synthetic"
	slow.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}, slow}, store, dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	var clockMu sync.Mutex
	now := opened
	controller.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	dispatcher.clock = controller.clock
	controller.gitRead = fakeArchitecture("")
	broker := &riskBroker{}
	controller.broker = broker
	healedAt := opened.Add(20 * time.Second)
	for step := 0; step < 1200 && len(broker.closures()) == 0; step++ {
		clockMu.Lock()
		at := now
		clockMu.Unlock()
		if !at.Before(healedAt) {
			world.healthy.Store(true)
		}
		if err := controller.Step(ctx, at, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := controller.PendingWorkspaceEffect(); ok {
			if err := controller.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-controller.workspaceResults
			controller.workspaceResults <- completion
		} else if effect, ok := controller.PendingDispatchEffect(); ok {
			if err := controller.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			// The responder runs in a goroutine; the scenario starts once it is working.
			for !dispatcher.hasStarted() {
				runtime.Gosched()
			}
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			// The broker call runs in a goroutine. Wait for it before the
			// virtual clock moves on, so the elapsed time below is the
			// controller's decision latency, not goroutine scheduling.
			awaitEffectGoroutines(controller)
		}
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
	}
	closures := broker.closures()
	if len(closures) == 0 {
		t.Fatalf("self-healed incident never closed: open=%v dispatch=%s epoch=%s mutation=%s history=%v", controller.incidentOpen, controller.dispatchState, controller.recoveryObservationEpochLocked(), controller.earlyLastMutationAt, controller.history[len(controller.history)-4:])
	}
	closure := closures[0]
	elapsed := controller.clock().Sub(healedAt) - terminating
	if elapsed > time.Minute {
		t.Fatalf("closure took %s after the workload healed", elapsed)
	}
	if closure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(closure.FinalDetectorStates) == 0 {
		t.Fatalf("self-healed closure must be verified: reason=%s finals=%d", closure.UnconfirmedReason, len(closure.FinalDetectorStates))
	}
	outcome, err := ProtoOutcomeDeriver{}.Derive(ctx, closure, "", "synthetic", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if got := outcome.Record.GetClassification(); got != "cleared_without_action" {
		t.Fatalf("classification = %s, want cleared_without_action", got)
	}
}

// The verification timeout bounds how long findings or unconfirmed observation
// can hold an incident open. With every health detector clear and nothing
// pending, only the stability window remains; it must complete instead of the
// timeout closing the incident unconfirmed.
func TestVerificationTimeoutDoesNotApplyWithoutActiveDetectors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	d := controllerDetector("health", time.Second, sdk.Finding{})
	d.spec.Class = sdk.DetectorClassHealth
	d.spec.Owner = sdk.DetectorOwnerHealthJudge
	d.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	d.spec.OriginatingCommit = "synthetic"
	d.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = 30 * time.Second
	cfg.VerificationTimeout = 10 * time.Second
	c, err := NewController(cfg, []sdk.Detector{d}, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = func() time.Time { return now }
	c.incidentOpen, c.responderDone = true, true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", ResponseDeadline: now.Add(time.Hour)}
	c.incidentDetectedAt, c.incidentDispatchedAt, c.responderCompletedAt = now.Add(-3*time.Second), now.Add(-2*time.Second), now.Add(-time.Second)
	result := completedResult("synthetic")
	c.currentIncidentResult = &result
	var closed *IncidentClosure
	c.OnIncidentClosed = func(closure IncidentClosure) { closed = &closure }
	for i := 0; i < 90 && closed == nil; i++ {
		if err := c.Step(ctx, now, nil); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	if closed == nil {
		t.Fatal("incident never closed")
	}
	if closed.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(closed.FinalDetectorStates) == 0 {
		t.Fatalf("closed unconfirmed by the verification timeout: reason=%s finals=%d", closed.UnconfirmedReason, len(closed.FinalDetectorStates))
	}
}
