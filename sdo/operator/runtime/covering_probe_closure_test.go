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

// coveringProbe is a health probe whose operation crosses the faulted
// component: users keep failing after the component's pod is Ready again (a
// credential the restart did not fix), until usersOK.
type coveringProbe struct{ usersOK *atomic.Bool }

func (coveringProbe) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "traffic-probe-call", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"health-objective"}, OriginatingCommit: "synthetic"}
}

func (p coveringProbe) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if p.usersOK.Load() {
		return []sdk.Finding{}, nil
	}
	return []sdk.Finding{{DetectorID: "traffic-probe-call", RuleID: "scenario-slo.call-repository", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: view.Namespace(), Name: "api"},
		Summary: "repository read fails", Evidence: "synthetic 500", Playbooks: []string{"health-objective"}}}, nil
}

// A component's pod restarts (the incident's own finding) and comes back
// Ready, but the probe whose operation crosses that component keeps failing:
// the restart alone is no recovery. The incident stays open until the
// covering probe passes again, and only then closes.
func TestClosureWaitsForTheProbeCoveringTheFaultedComponent(t *testing.T) {
	ctx := context.Background()
	opened := dataRiskOpened
	world := &sameWriteWorld{}
	store := newRestartedStore(opened)
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.FiringThreshold, cfg.ClearThreshold = 1, 1
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.VerificationTimeout = 10 * time.Minute
	dispatcher := &blockingDispatcher{stopAfter: opened.Add(20 * time.Second)}
	usersOK := &atomic.Bool{}
	usersOK.Store(true)
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: world}, coveringProbe{usersOK: usersOK}}, store, dispatcher, opened)
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
	podReadyAt, usersRecoverAt := opened.Add(20*time.Second), opened.Add(3*time.Minute)
	for step := 0; step < 1200 && len(broker.closures()) == 0; step++ {
		clockMu.Lock()
		at := now
		clockMu.Unlock()
		switch {
		case !at.Before(usersRecoverAt):
			usersOK.Store(true)
		case !at.Before(podReadyAt):
			// The pod is Ready again; its requests still fail.
			world.healthy.Store(true)
			usersOK.Store(false)
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
		t.Fatal("incident never closed after the covering probe passed")
	}
	if closedAt := controller.clock(); closedAt.Before(usersRecoverAt) {
		t.Fatalf("closed at %s, %s before the covering probe passed again", closedAt, usersRecoverAt.Sub(closedAt))
	}
	for _, state := range closures[0].FinalDetectorStates {
		if state.DetectorID == "traffic-probe-call" {
			return
		}
	}
	t.Fatalf("the covering probe is not among the closure's detector states: %+v", closures[0].FinalDetectorStates)
}
