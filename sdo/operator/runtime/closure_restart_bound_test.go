package runtime

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// hpaStatusChanges plays a HorizontalPodAutoscaler controller refreshing its
// status: every Take delivers one status-only update, a write SDO did not make.
type hpaStatusChanges struct {
	t       *testing.T
	refresh int32
}

func (q *hpaStatusChanges) TakeObjectChanges() []ObjectChange {
	hpa := func(replicas int32) *autoscalingv2.HorizontalPodAutoscaler {
		return &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "worker", Namespace: "app"},
			Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 4},
			Status:     autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: replicas, DesiredReplicas: replicas},
		}
	}
	q.refresh++
	return []ObjectChange{objectChange(q.t, hpa(q.refresh), hpa(q.refresh+1), "autoscaling/v2", "HorizontalPodAutoscaler")}
}

type closureRestartWorld struct {
	controller *Controller
	broker     *riskBroker
	world      *sameWriteWorld
	dispatcher *blockingDispatcher
	mu         sync.Mutex
	now        time.Time
	healedAt   time.Time
}

// newClosureRestartWorld is a self-healing incident whose stability window is
// longer than one controller step. The controller clock advances a little on
// every read, like a real clock: two reads in one pass are never equal.
func newClosureRestartWorld(t *testing.T) *closureRestartWorld {
	t.Helper()
	opened := dataRiskOpened
	w := &closureRestartWorld{world: &sameWriteWorld{}, broker: &riskBroker{}, now: opened, healedAt: opened.Add(20 * time.Second)}
	cfg := testControllerConfig()
	cfg.Namespace, cfg.SourceCommit, cfg.RepositoryWorktree = "app", "synthetic-accepted", "synthetic-worktree"
	cfg.ClosureStabilityWindow = 15 * time.Second
	cfg.VerificationTimeout = 5 * time.Minute
	w.dispatcher = &blockingDispatcher{stopAfter: w.healedAt}
	slow := controllerDetector("slow-probe", 10*time.Minute, sdk.Finding{})
	slow.spec.Class, slow.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	slow.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	slow.spec.OriginatingCommit = "synthetic"
	slow.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	controller, err := NewController(cfg, []sdk.Detector{sameWriteDetector{world: w.world}, slow}, newRestartedStore(opened), w.dispatcher, opened)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.now = w.now.Add(time.Microsecond)
		return w.now
	}
	w.dispatcher.clock = controller.clock
	controller.gitRead = fakeArchitecture("")
	controller.broker = w.broker
	w.controller = controller
	return w
}

// run drives the controller one second per step until the broker receives a
// closure or steps run out. beforeClosure runs before every closure pass.
func (w *closureRestartWorld) run(t *testing.T, steps int, beforeClosure func()) {
	t.Helper()
	w.runUntil(t, steps, beforeClosure, func() bool { return len(w.broker.closures()) > 0 })
}

// finalized reports a closure that left the pending state, verified or not.
func (w *closureRestartWorld) finalized() bool {
	w.controller.mu.Lock()
	defer w.controller.mu.Unlock()
	state := w.controller.closureState
	return len(w.broker.closures()) > 0 || state != contracts.ClosureState_CLOSURE_STATE_UNSPECIFIED && state != contracts.ClosureState_CLOSURE_STATE_PENDING
}

func (w *closureRestartWorld) runUntil(t *testing.T, steps int, beforeClosure func(), done func() bool) {
	t.Helper()
	ctx := context.Background()
	c := w.controller
	for step := 0; step < steps && !done(); step++ {
		at := c.clock()
		if !at.Before(w.healedAt) {
			w.world.healthy.Store(true)
		}
		if err := c.Step(ctx, at, nil); err != nil {
			t.Fatal(err)
		}
		if effect, ok := c.PendingWorkspaceEffect(); ok {
			if err := c.ExecuteWorkspaceEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			completion := <-c.workspaceResults
			c.workspaceResults <- completion
		} else if effect, ok := c.PendingDispatchEffect(); ok {
			if err := c.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			for !w.dispatcher.hasStarted() {
				runtime.Gosched()
			}
		} else if effect, ok := c.PendingClosureEffect(); ok {
			if beforeClosure != nil {
				beforeClosure()
			}
			if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			awaitEffectGoroutines(c)
		}
		w.mu.Lock()
		w.now = w.now.Add(time.Second)
		w.mu.Unlock()
	}
}

// A busy application's own controllers write all the time (an HPA refreshes
// its status every few seconds). Those are not SDO's writes, so they never
// restart the stability window: a healed incident closes verified about one
// window after its alerts cleared.
func TestUnrelatedObjectChangesDoNotRestartClosureStability(t *testing.T) {
	w := newClosureRestartWorld(t)
	w.controller.ConfigureDrift(DriftControllerConfig{Events: &hpaStatusChanges{t: t}})
	w.run(t, 1200, nil)
	closures := w.broker.closures()
	if len(closures) == 0 {
		w.controller.mu.Lock()
		defer w.controller.mu.Unlock()
		t.Fatalf("healed incident never closed while an HPA refreshed its status: closure=%s stable_since=%s safety_lost=%v",
			w.controller.closureState, w.controller.closureStableSince, w.controller.recoverySafetyLost)
	}
	if elapsed := w.controller.clock().Sub(w.healedAt); elapsed > time.Minute {
		t.Fatalf("closure took %s after the workload healed", elapsed)
	}
	if closures[0].UnconfirmedReason != contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || len(closures[0].FinalDetectorStates) == 0 {
		t.Fatalf("healed closure must be verified: reason=%s finals=%d", closures[0].UnconfirmedReason, len(closures[0].FinalDetectorStates))
	}
}

// Whatever restarts the stability window, a window that restarts at every
// pass never completes, so it is not "running" and the verification deadline
// still ends the closure wait. No restart source can hold a closure forever.
func TestVerificationDeadlineEndsAClosureWhoseWindowRestartsEveryPass(t *testing.T) {
	w := newClosureRestartWorld(t)
	w.runUntil(t, 1200, func() { w.controller.resetEarlyStability() }, w.finalized)
	if !w.finalized() {
		w.controller.mu.Lock()
		defer w.controller.mu.Unlock()
		t.Fatalf("a closure whose window restarts every pass was never finalized: closure=%s stable_since=%s", w.controller.closureState, w.controller.closureStableSince)
	}
	if elapsed := w.controller.clock().Sub(w.healedAt); elapsed > 7*time.Minute {
		t.Fatalf("closure took %s after the workload healed, past the 5m verification deadline", elapsed)
	}
}

// A write SDO knows about (the responder's observed mutation moves the
// observation epoch) still restarts the window: the proof counts the full
// window from that write, not from the alerts clearing.
func TestAKnownWriteRestartsTheStabilityWindow(t *testing.T) {
	const id = "traffic-probe-read-later"
	cleared := time.Unix(100, 0).UTC()
	window := 15 * time.Second
	c := openersController(t, &recordingDispatcher{})
	c.config.ClosureStabilityWindow = window
	c.recoverySafetyLost = false
	scope := newRecoveryScope([]string{FindingStateKey(id, "x")})
	c.recordEvaluation(id, cleared, nil, scope)
	write := cleared.Add(10 * time.Second)
	c.mu.Lock()
	c.earlyLastMutationAt = write
	c.mu.Unlock()
	for _, tc := range []struct {
		at   time.Time
		want bool
	}{{write.Add(window - time.Second), false}, {write.Add(window), true}} {
		c.recordEvaluation(id, tc.at, nil, scope)
		states, ok := c.latestEvaluations([]string{id}, time.Time{}, true)
		if !ok {
			t.Fatal("latest evaluation is not clear")
		}
		c.mu.Lock()
		got := c.recoveryProofLocked(tc.at, states, true)
		c.mu.Unlock()
		if got != tc.want {
			t.Fatalf("proof at write+%s = %v, want %v: the window must count from the known write", tc.at.Sub(write), got, tc.want)
		}
	}
}
