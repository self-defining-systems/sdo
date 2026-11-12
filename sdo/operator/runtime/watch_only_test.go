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
	ctx := context.Background()
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
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
	broker := &riskBroker{}
	controller.broker = broker
	healedAt := opened.Add(90 * time.Second)
	for step := 0; step < 600 && len(broker.closures()) == 0; step++ {
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
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
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
	if elapsed := controller.clock().Sub(healedAt); elapsed > time.Minute {
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
