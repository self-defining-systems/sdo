package runtime

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// heldApplyDispatcher is the production shape of an automatic-stage
// transport: the response pipeline runs the deterministic stage over loop,
// and an agent dispatch is counted and runs until its context ends.
type heldApplyDispatcher struct {
	loop   *ResponderLoop
	agents atomic.Int32
}

func (d *heldApplyDispatcher) DeterministicResponseStage() ResponseStage {
	return &DeterministicResponseStage{NewLoop: func(IncidentRequest) *ResponderLoop { return d.loop }, KeepLoop: true}
}

func (d *heldApplyDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.agents.Add(1)
	<-ctx.Done()
	return completedResult(request.IncidentID), ctx.Err()
}

// heldRollouts holds the stage's first rollout observation, inside its
// apply, until the world releases it.
type heldRollouts struct{ w *heldApplyWorld }

func (r heldRollouts) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.w.held.CompareAndSwap(false, true) {
		close(r.w.writing)
		<-r.w.release
	}
	return r.w.world.Observe(ctx, refs)
}

func (r heldRollouts) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	return r.w.world.Wait(ctx, refs)
}

// countedProbe counts its evaluations.
type countedProbe struct {
	*switchedProbe
	evaluations atomic.Int32
}

func (d *countedProbe) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	d.evaluations.Add(1)
	return d.switchedProbe.Detect(ctx, view)
}

// heldApplyWorld is an incident whose automatic stage is applying a restart
// of api whose rollout observation is held until release is closed (a slow
// rollout poll, or a script up to its timeout). The controller steps once a second of bubble time.
type heldApplyWorld struct {
	t          *testing.T
	sleep      func(time.Duration)
	c          *Controller
	world      *sameWriteWorld
	dispatcher *heldApplyDispatcher
	joiner     *countedProbe
	writing    chan struct{}
	release    chan struct{}
	released   sync.Once
	held       atomic.Bool
}

// now and sleep are the bubble's clock.
func newHeldApplyWorld(t *testing.T, now func() time.Time, sleep func(time.Duration)) *heldApplyWorld {
	t.Helper()
	ctx := t.Context()
	w := &heldApplyWorld{t: t, sleep: sleep, world: e2eWorld(), writing: make(chan struct{}), release: make(chan struct{})}
	route := &e2eRoute{}
	route.shown.Store(true)
	w.joiner = &countedProbe{switchedProbe: &switchedProbe{id: memory.TrafficDetectorPrefix + "browse", scenario: "browse"}}
	detectors := []sdk.Detector{route, w.joiner}
	// A learned playbook restarts api; the opening route names it.
	restore := componentRestart{id: "fix"}
	registry, err := program.NewRegistry([]program.Diagnoser{restore}, []program.Mitigator{restore})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.RequestMetricsOpens = true
	cfg.CorpusSessionRoot = t.TempDir()
	w.dispatcher = &heldApplyDispatcher{}
	c, err := NewController(cfg, detectors, w.world, w.dispatcher, now())
	if err != nil {
		t.Fatal(err)
	}
	w.c = c
	c.now = now
	snapshot, err := w.world.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opening, _ := route.Detect(ctx, snapshot)
	c.tracker.Observe(c.clock(), route.Spec().ID, opening)
	request := IncidentRequest{IncidentID: "synthetic-held-apply", Application: "demo", Namespace: "app", SchemaVersion: ProtocolSchemaVersion,
		SourceCommit: "synthetic", DeployedCommit: "synthetic", RepositoryBaseCommit: "synthetic", RepositoryWorktree: "synthetic-worktree",
		CancellationToken: "synthetic-cancel", RepairPolicy: "recorded-actions", Findings: opening, OpeningKeys: findingKeys(opening),
		DetectedAt: c.clock(), ResponseDeadline: c.clock().Add(30 * time.Minute)}
	executor := PlanExecutor{Client: TypedPlanClient{Client: w.world.client}, Source: w.world, Rollouts: heldRollouts{w}, Verifier: PlanHealthVerifier{Detectors: detectors, Provider: w.world}}
	loop := NewResponderLoop(request, registry, w.world, detectors, executor, 5)
	loop.Now = c.clock
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)} }
	loop.saveToolState = func(context.Context, []byte) error { return nil }
	w.dispatcher.loop = loop
	t.Cleanup(w.releaseApply)
	c.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	c.responderLoop = loop
	c.incidentOpen, c.dispatchState = true, contracts.DispatchState_DISPATCH_STATE_PENDING
	c.currentIncidentRequest = &request
	c.incidentFindingKeys = findingKeys(opening)
	c.incidentDetectedAt = c.clock()
	return w
}

// start launches the response pipeline and returns once the stage's write is
// written and the apply is held.
func (w *heldApplyWorld) start() {
	w.t.Helper()
	effect, ok := w.c.PendingDispatchEffect()
	if !ok {
		w.t.Fatal("no pending dispatch")
	}
	if err := w.c.ExecuteDispatchEffect(w.t.Context(), effect); err != nil {
		w.t.Fatal(err)
	}
	synctest.Wait()
	select {
	case <-w.writing:
	default:
		w.c.processDispatchCompletions(w.c.clock())
		w.c.mu.Lock()
		defer w.c.mu.Unlock()
		w.t.Fatalf("the stage did not write: %v, agents %d", deterministicStageReport(w.c.currentIncidentRequest.PriorStages), w.dispatcher.agents.Load())
	}
}

// releaseApply lets the held apply return.
func (w *heldApplyWorld) releaseApply() {
	w.released.Do(func() { close(w.release) })
	synctest.Wait()
}

// step advances one second and runs one controller pass. A pass that waits
// on the held apply never returns: the bubble cannot advance past it.
func (w *heldApplyWorld) step() {
	w.t.Helper()
	w.sleep(time.Second)
	if err := w.c.Step(w.t.Context(), w.c.clock(), nil); err != nil {
		w.t.Fatal(err)
	}
	synctest.Wait()
	if effect, ok := w.c.PendingDispatchEffect(); ok {
		if err := w.c.ExecuteDispatchEffect(w.t.Context(), effect); err != nil {
			w.t.Fatal(err)
		}
		synctest.Wait()
	}
}

func (w *heldApplyWorld) joined() bool {
	w.c.mu.Lock()
	defer w.c.mu.Unlock()
	return w.c.currentIncidentRequest != nil && slices.Contains(w.c.currentIncidentRequest.JoinedKeys, findingKey(w.joiner.finding()))
}

// Detection keeps its cadence while the automatic stage applies: a long
// apply (a script up to its timeout, a slow rollout poll) holds the
// responder loop, never the controller's observation. A user test that
// starts failing during the apply is evaluated, joins the alert set and is
// captured within a second, as it would be with no apply running.
func TestDetectorsKeepEvaluatingDuringALongStageApply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newHeldApplyWorld(t, time.Now, time.Sleep)
		w.start()
		before := w.joiner.evaluations.Load()
		w.step()
		w.joiner.failing.Store(true)
		w.step()
		if !w.joined() {
			t.Fatal("a user test that started failing during the apply did not join the alert set at its first failing evaluation")
		}
		for range 3 * 60 {
			w.step()
		}
		if got := w.joiner.evaluations.Load() - before; got < 3*60 {
			t.Fatalf("the joiner's detector evaluated %d times in 3 minutes of a held apply, want once a second", got)
		}
		if !w.held.Load() || w.world.restarts.Load() != 1 {
			t.Fatal("the apply was not held for the whole time")
		}
	})
}

// The stage is the one writer until its apply returns: while the apply is
// held, no observation dispatches the agent, however long the alerts fail or
// whatever joins. Once it returns, its write fixes the opening route but the
// joiner keeps failing, so the stage hands the incident off and the agent
// starts exactly once, after the apply, with the joiner in its alert set.
func TestNoAgentDispatchRacesAHeldStageApply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newHeldApplyWorld(t, time.Now, time.Sleep)
		w.start()
		w.joiner.failing.Store(true)
		for range 4 * 60 {
			w.step()
			if n := w.dispatcher.agents.Load(); n != 0 {
				t.Fatalf("the agent was dispatched %d times while the stage's apply was held", n)
			}
		}
		if !w.joined() {
			t.Fatal("the joiner did not join while the stage owned the repair")
		}
		released := w.c.clock()
		w.releaseApply()
		for range 30 * 60 {
			if w.dispatcher.agents.Load() > 0 {
				break
			}
			w.step()
		}
		for range 10 {
			w.step()
		}
		w.c.mu.Lock()
		report := deterministicStageReport(w.c.currentIncidentRequest.PriorStages)
		keys := alertKeys(w.c.currentIncidentRequest)
		w.c.mu.Unlock()
		if n := w.dispatcher.agents.Load(); n != 1 || !report.GetHandoffSelected() || report.GetApplyCompletedAt().AsTime().Before(released) {
			t.Fatalf("%d agent dispatches, report %v: want one hand-off after the apply returned at %s", n, report, released)
		}
		if !slices.Contains(keys, findingKey(w.joiner.finding())) || w.world.restarts.Load() != 1 {
			t.Fatalf("alert set %v, %d stage writes: want the joiner kept and one write", keys, w.world.restarts.Load())
		}
	})
}
