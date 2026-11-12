package runtime

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// openingTestDetector is the traffic detector of the user test the incident
// opens on. Its own SLO window clears early (it still holds passing samples
// from before the fault), while the fresh samples keep failing.
type openingTestDetector struct{ firing *atomic.Bool }

func (openingTestDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "synthetic"}
}

func (d openingTestDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	if !d.firing.Load() {
		return []sdk.Finding{}, nil
	}
	finding := browseTest()
	finding.Summary, finding.Evidence = "browse-items fails", "synthetic 500"
	return []sdk.Finding{finding}, nil
}

// liveVerifyWorld serves the verify world's traffic windows at the
// controller's current time.
type liveVerifyWorld struct {
	clock   func() time.Time
	probeOK func(time.Time) bool
}

func (w liveVerifyWorld) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return verifyWorld{now: w.clock(), probeOK: w.probeOK}.view(), nil
}

// The opening user test's detector clears, but the test's fresh samples still
// fail: that is not Recovered, so the incident neither closes nor marks the
// responder verified. Once the test passes in a sustained way it closes.
func TestClearedFindingsDoNotCloseWhileTheOpeningUserTestFails(t *testing.T) {
	ctx := context.Background()
	opened := verifyT0.Add(time.Minute)
	recoverAt := opened.Add(3 * time.Minute)
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 10 * time.Minute
	dispatcher := &blockingDispatcher{}
	firing := &atomic.Bool{}
	firing.Store(true)
	var clockMu sync.Mutex
	now := opened
	clock := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return now }
	world := liveVerifyWorld{clock: clock, probeOK: func(at time.Time) bool { return at.Before(verifyT0.Add(30*time.Second)) || !at.Before(recoverAt) }}
	controller, err := NewController(cfg, []sdk.Detector{openingTestDetector{firing: firing}}, world, dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = clock
	dispatcher.clock = controller.clock
	controller.gitRead = fakeArchitecture(verifyArch)
	broker := &riskBroker{}
	controller.broker = broker
	for step := 0; step < 600 && len(broker.closures()) == 0; step++ {
		at := clock()
		if !at.Before(opened.Add(90 * time.Second)) {
			firing.Store(false)
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
			for !dispatcher.hasStarted() {
				runtime.Gosched()
			}
		} else if effect, ok := controller.PendingClosureEffect(); ok {
			if at.Before(recoverAt) {
				t.Fatalf("closure began at %s while the opening user test still failed", at)
			}
			if err := controller.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(controller)
		}
		clockMu.Lock()
		now = now.Add(time.Second)
		clockMu.Unlock()
	}
	if len(broker.closures()) == 0 {
		t.Fatalf("incident never closed after the opening user test passed again: open=%v state=%s closure=%v summary=%+v", controller.incidentOpen, controller.dispatchState, controller.closureState, controller.cachedVerification)
	}
}
