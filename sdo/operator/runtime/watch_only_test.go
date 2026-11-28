package runtime

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// A watch-only controller detects, opens, verifies and closes an incident as
// usual, but never responds: the configured dispatcher is never called, no
// automatic stage is selected and no dispatch is recorded in lifecycle events.
func TestWatchOnlyIncidentClosesOnHealthWithoutResponding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 30 * time.Second
	cfg.ResponseTimeout = 5 * time.Hour
	cfg = cfg.WithMode(settings.ModeWatchOnly)
	responder := &recordingDispatcher{requests: make(chan IncidentRequest, 8)}
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}}, newRestartedStore(opened), responder, opened)
	if err != nil {
		t.Fatal(err)
	}
	if got := controller.codePath.Selection.ResponseStages; !slices.Equal(got, []string{"agent"}) {
		t.Fatalf("watch-only response stages = %v, want the agent stage alone (no automatic stage)", got)
	}
	var clockMu sync.Mutex
	now := opened
	controller.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	controller.gitRead = fakeArchitecture("")
	var lifecycleMu sync.Mutex
	var events []string
	controller.SetLifecycleSink(lifecycleSinkFunc(func(event *contracts.LifecycleEvent) {
		lifecycleMu.Lock()
		defer lifecycleMu.Unlock()
		events = append(events, event.GetEvent())
	}))
	broker := &watchOnlyTestBroker{}
	controller.broker = broker
	healedAt := opened.Add(90 * time.Second)
	var firstAcknowledgedAt time.Time
	for step := 0; step < 300; step++ {
		clockMu.Lock()
		at := now
		clockMu.Unlock()
		world.healthy.Store(!at.Before(healedAt) && firstAcknowledgedAt.IsZero())
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
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		} else if effect, ok := controller.PendingClosureAcknowledgmentEffect(); ok {
			if err := controller.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		}
		if first := controller.LastAcknowledgedIncidentID(); first != "" {
			if firstAcknowledgedAt.IsZero() {
				firstAcknowledgedAt = at
			}
			if controller.incidentOpen && controller.currentIncidentRequest.IncidentID != first {
				break
			}
		}
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
	}
	closures := broker.closures()
	if len(closures) == 0 {
		t.Fatalf("watch-only incident never closed: open=%v dispatch=%s", controller.incidentOpen, controller.dispatchState)
	}
	if calls := responder.callCount(); calls != 0 {
		t.Fatalf("watch-only controller called the responder %d times", calls)
	}
	closure := closures[0]
	if closure.UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(closure.FinalDetectorStates) == 0 {
		t.Fatalf("watch-only closure must be health-verified: reason=%s finals=%d", closure.UnconfirmedReason, len(closure.FinalDetectorStates))
	}
	if closure.Request.IncidentID == "" || len(closure.Request.Findings) == 0 || closure.DetectedAt.IsZero() {
		t.Fatalf("watch-only closure lost its incident record: %+v", closure.Request)
	}
	if firstAcknowledgedAt.IsZero() || !controller.incidentOpen || controller.currentIncidentRequest.IncidentID == closure.Request.IncidentID {
		t.Fatal("second alert did not open a new incident after acknowledgement")
	}
	if closure.Request.SessionGeneration != 0 {
		t.Fatalf("watch-only closure restarted its session %d times", closure.Request.SessionGeneration)
	}
	if elapsed := firstAcknowledgedAt.Sub(healedAt); elapsed > time.Minute {
		t.Fatalf("closure took %s after the workload healed", elapsed)
	}
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	if slices.Contains(events, PhaseDispatched) {
		t.Fatalf("watch-only controller recorded a dispatch: %v", events)
	}
	if !slices.Contains(events, PhaseClosed) {
		t.Fatalf("watch-only controller recorded no closure: %v", events)
	}
}

func TestWatchOnlyRestartResumesLegacyPendingClosure(t *testing.T) {
	// A blocking dispatcher produces the same persisted report wait as the
	// old watch-only dispatcher.
	world := newClosureRestartWorld(t)
	original := world.controller
	world.runUntil(t, 45, nil, func() bool { return original.earlyReportPendingLocked() })
	state, err := original.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	original.responderCancel()
	original.Wait()
	if !state.GetPendingClosure().GetVerifiedWhileResponderRunning() {
		t.Fatal("the legacy controller did not reach the pending report wait")
	}
	now := original.clock()
	restarted, err := NewController(original.config.WithMode(settings.ModeWatchOnly),
		[]sdk.Detector{sameWriteDetector{world: world.world}}, newRestartedStore(now), nil, now)
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return now }
	restarted.gitRead = fakeArchitecture("")
	restarted.broker = world.broker
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if id, err := restarted.ResetIncidents(ctx, now); err != nil || id != "" {
		t.Fatalf("reset during pending closure = %q, %v", id, err)
	}
	if err := restarted.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.PendingClosureEffect(); !ok {
		t.Fatal("restarted controller is still waiting for a responder")
	}
}

type watchOnlyTestBroker struct{ riskBroker }

func (b *watchOnlyTestBroker) ProcessClosure(ctx context.Context, closure IncidentClosure) (ClosureReceipt, error) {
	receipt, err := b.riskBroker.ProcessClosure(ctx, closure)
	receipt.OutcomeCommit = "synthetic-accepted"
	receipt.AckToken = "synthetic-ack"
	return receipt, err
}

func TestWatchOnlyRejectsExternalIncidents(t *testing.T) {
	cfg := testControllerConfig()
	cfg.WatchOnly, cfg.ExternalIncidentsOnly = true, true
	_, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, time.Unix(1000, 0))
	if err == nil || !strings.Contains(err.Error(), "watch-only") {
		t.Fatalf("NewController error = %v, want a watch-only conflict", err)
	}
}

// The mode is configuration only: the launch carries no mode flag for the
// child to parse.
func TestModeIsNotALaunchFlag(t *testing.T) {
	for _, mode := range settings.Modes {
		cfg := testControllerConfig().WithMode(mode)
		if cfg.WatchOnly != (mode == settings.ModeWatchOnly) || cfg.ExternalIncidentsOnly != (mode == settings.ModeExternal) {
			t.Fatalf("mode %s = %+v", mode, cfg)
		}
	}
	for _, arg := range controllerConfigToArgs(&contracts.ControllerConfig{}) {
		if strings.Contains(arg, "watch-only") || strings.Contains(arg, "frozen") || strings.Contains(arg, "external") {
			t.Fatalf("launch args carry a mode: %v", arg)
		}
	}
}

type lifecycleSinkFunc func(*contracts.LifecycleEvent)

func (f lifecycleSinkFunc) RecordLifecycle(event *contracts.LifecycleEvent) error {
	f(event)
	return nil
}
