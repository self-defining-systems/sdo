package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Drive the real stage and closure over the same synthetic clock and live
// rollout snapshots. The controller observes only when the test steps it.
func automaticRecoveryWorld(t *testing.T, tc settlingCase, policy string, extra ...sdk.Detector) (*Controller, *settlingWorld, *IncidentClosure) {
	t.Helper()
	world := newSettlingWorld(tc)
	stage, state := settlingStageOn(t, tc, world)
	loop := stage.NewLoop(state.Request)
	now := world.clock.Now()
	request := state.Request
	request.Application, request.SchemaVersion = "demo", ProtocolSchemaVersion
	request.DetectedAt, request.ResponseDeadline = now, now.Add(10*time.Minute)
	request.DeployedCommit = request.SourceCommit
	request.RepositoryBaseCommit = request.SourceCommit
	request.RepositoryWorktree = "synthetic-worktree"
	request.CancellationToken = "synthetic-cancel"
	request.DetectorHistory = []DetectorEvaluation{{DetectorID: "traffic-health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}
	for _, f := range request.Findings {
		if f.DetectorID == "traffic-health" {
			request.OpeningKeys = append(request.OpeningKeys, findingKey(f))
		}
	}
	loop.Request, state.Request = request, request
	cfg := testControllerConfig()
	cfg.Namespace = "app"
	cfg.ClosureStabilityWindow = 5 * time.Second
	cfg.AfterPlaybook = policy
	cfg.AfterPlaybookWait = 4 * time.Minute
	if policy == settings.AfterPlaybookWait {
		stage.WaitBound = cfg.AfterPlaybookWait
	}
	stage.StabilityWindow, stage.EstimateBound = cfg.ClosureStabilityWindow, cfg.AfterPlaybookWait
	controller, err := NewController(cfg, append(slices.Clone(loop.Detectors[:1]), extra...), world, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = world.clock.Now
	controller.codePath = &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}
	controller.responderLoop = loop
	controller.incidentOpen = true
	controller.incidentDetectedAt, controller.incidentDispatchedAt = now, now
	controller.currentIncidentRequest = &request
	report, err := stage.Run(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	state.PriorStages = []*contracts.StageReport{report}
	result, err := responsePipelineResult(state, now, world.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	controller.handleDispatchCompletion(dispatchCompletion{incidentID: request.IncidentID, result: result, priorStages: state.PriorStages}, world.clock.Now())
	closed := new(IncidentClosure)
	controller.OnIncidentClosed = func(closure IncidentClosure) { *closed = closure }
	return controller, world, closed
}

func autoStep(t *testing.T, c *Controller, clock *settlingClock, advance time.Duration) {
	t.Helper()
	_ = clock.advance(t.Context(), advance)
	c.maybeCloseIncident(t.Context(), clock.Now(), nil)
	c.Wait()
}

func autoReport(c *Controller) *contracts.StageReport {
	return deterministicStageReport(c.currentIncidentRequest.PriorStages)
}

func TestAutomaticRecoveryWaitsOnlyForClosure(t *testing.T) {
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 93 * time.Second}, settings.AfterPlaybookAgent)
	initial := autoReport(c).GetRecoveryDeadline().AsTime()
	if want := world.clock.Now().Add(95 * time.Second); !initial.Equal(want) {
		t.Fatalf("deadline = %s, want %s", initial, want)
	}
	autoStep(t, c, world.clock, 93*time.Second)
	autoStep(t, c, world.clock, 2*time.Second)
	report := autoReport(c)
	extension := report.GetStabilityDeadline().AsTime()
	if !extension.Equal(world.clock.Now().Add(4*time.Second)) || !report.GetRecoveryDeadline().AsTime().Equal(initial) {
		t.Fatalf("deadline and extension = %s, %s", report.GetRecoveryDeadline(), report.GetStabilityDeadline())
	}
	if report.GetHandoffSelected() || !c.escalatedAt.IsZero() || report.GetMitigations()[0].GetRolloutUnsettledAtDeadline() {
		t.Fatal("a stability-only wait handed off, escalated or debited trust")
	}
	autoStep(t, c, world.clock, 3*time.Second)
	autoStep(t, c, world.clock, time.Second)
	if closed.Result == nil || deterministicStageReport(closed.Request.PriorStages).GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("closure did not award the stage its recovery result: %+v", closed)
	}
	if world.restarts.Load() != 1 {
		t.Fatalf("the stage wrote %d times", world.restarts.Load())
	}
}

func TestAutomaticRecoveryDeadlineSurvivesRestartAndWaitPolicy(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{startup: 40 * time.Second}, settings.AfterPlaybookWait)
	bound := world.clock.Now().Add(4 * time.Minute)
	if got := autoReport(c).GetRecoveryDeadline().AsTime(); !got.Equal(bound) {
		t.Fatalf("wait deadline = %s, want %s", got, bound)
	}
	autoStep(t, c, world.clock, 3*time.Minute)
	checkpoint, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	loop := c.responderLoop
	restarted, err := NewController(c.config, loop.Detectors[:1], world, c.dispatcher, world.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.now, restarted.codePath = world.clock.Now, c.codePath
	if err := restarted.RestoreState(checkpoint); err != nil {
		t.Fatal(err)
	}
	autoStep(t, restarted, world.clock, 59*time.Second)
	if got := autoReport(restarted).GetRecoveryDeadline().AsTime(); !got.Equal(bound) || !restarted.escalatedAt.IsZero() {
		t.Fatalf("wait deadline moved or expired before its bound: %s", got)
	}
	autoStep(t, restarted, world.clock, time.Second)
	if !restarted.escalatedAt.Equal(bound) || autoReport(restarted).GetHandoffSelected() || restarted.awaitingAutomaticRecoveryLocked() != nil || world.restarts.Load() != 1 {
		t.Fatal("the configured wait did not expire without an agent or replay")
	}
}

func TestAutomaticRecoveryDeadlineAppliesPolicyToObservedAlerts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  string
		handoff bool
	}{
		{"one still firing", settings.AfterPlaybookAgent, true},
		{"escalate policy", settings.AfterPlaybookEscalate, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, world, _ := automaticRecoveryWorld(t, settlingCase{}, tc.policy)
			bound := autoReport(c).GetRecoveryDeadline().AsTime()
			autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
			if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
				t.Fatal("handoff or escalation before the fixed deadline")
			}
			autoStep(t, c, world.clock, time.Second)
			report := autoReport(c)
			if report.GetHandoffSelected() != tc.handoff || c.escalatedAt.IsZero() == !tc.handoff {
				t.Fatalf("handoff = %v, escalated at %s, want handoff = %v", report.GetHandoffSelected(), c.escalatedAt, tc.handoff)
			}
		})
	}
}

func setUpdatedReplicas(t *testing.T, world *settlingWorld, replicas int32) {
	t.Helper()
	deployments := world.client.AppsV1().Deployments("app")
	object, err := deployments.Get(t.Context(), "api", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Status.UpdatedReplicas = replicas
	if _, err := deployments.UpdateStatus(t.Context(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedAutomaticHandoffRetainsControllerAlertSet(t *testing.T) {
	now := verifyT0
	c, _ := afterPlaybookJoinedController(t, now, "")
	joined := c.detectors["traffic-hurt"].(*switchedProbe).finding()
	c.currentIncidentRequest.Findings = append(c.currentIncidentRequest.Findings, joined)
	c.currentIncidentRequest.JoinedKeys = []string{findingKey(joined)}
	autoReport(c).RecoveryDeadline = contractTimestamp(now)
	c.maybeCloseIncident(context.Background(), now, nil)
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the joined failure did not select handoff")
	}
	request := stageDispatchRequest(*c.currentIncidentRequest)
	if keys := findingKeys(request.Findings); !slices.Equal(keys, []string{findingKey(joined)}) || len(c.currentIncidentRequest.Findings) != 2 {
		t.Fatalf("residual = %v, controller findings = %v", keys, c.currentIncidentRequest.Findings)
	}
}

func TestAutomaticDeadlineIncludesStartupClearingLagAndResponseCap(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{startup: 40 * time.Second}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	now := world.clock.Now()
	if got, want := report.GetRecoveryDeadline().AsTime(), now.Add(135*time.Second); !got.Equal(want) {
		t.Fatalf("deadline with startup = %s, want %s", got, want)
	}
	view, err := world.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	long := windowedSettlingDetector{settlingDetector{id: "traffic-health", window: 2 * time.Minute}}
	request := *c.currentIncidentRequest
	got := automaticRecoveryDeadline(request, report, now, view, []sdk.Detector{long}, 0, 0, 5*time.Second)
	if want := now.Add(166 * time.Second); !got.Equal(want) {
		t.Fatalf("deadline with long clearing lag = %s, want %s", got, want)
	}
	request.ResponseDeadline = now.Add(time.Minute)
	if got := automaticRecoveryDeadline(request, report, now, view, []sdk.Detector{long}, 4*time.Minute, 0, 5*time.Second); !got.Equal(request.ResponseDeadline) {
		t.Fatalf("deadline = %s, want response cap %s", got, request.ResponseDeadline)
	}
}

func TestAutomaticHandoffClaimsOnceWhenHealthClearsBeforePublication(t *testing.T) {
	now := verifyT0
	c, _ := afterPlaybookJoinedController(t, now, "")
	probe := c.detectors["traffic-hurt"].(*switchedProbe)
	joined := probe.finding()
	c.currentIncidentRequest.Findings = append(c.currentIncidentRequest.Findings, joined)
	c.currentIncidentRequest.JoinedKeys = []string{findingKey(joined)}
	autoReport(c).RecoveryDeadline = contractTimestamp(now)
	c.maybeCloseIncident(t.Context(), now, nil)
	probe.failing.Store(false)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	c.dispatcher = dispatcher
	effect, ok := c.PendingDispatchEffect()
	if !ok {
		t.Fatal("handoff did not publish one pending dispatch")
	}
	if err := c.ExecuteDispatchEffect(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	if dispatcher.callCount() != 1 || len(dispatcher.requests) != 1 {
		t.Fatalf("claim count = %d with %d tasks", dispatcher.callCount(), len(dispatcher.requests))
	}
	if _, another := c.PendingDispatchEffect(); another {
		t.Fatal("selected handoff was dispatched twice")
	}
}

func TestDirectResponderClosureUsesItsExistingStabilityWindow(t *testing.T) {
	now := verifyT0
	clock := &settlingClock{now: now}
	world := &settlingDetector{id: "service-health", class: sdk.DetectorClassHealth, clock: clock, healAt: now.Add(time.Second)}
	findings := []sdk.Finding{{DetectorID: world.id, RuleID: world.id, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		PrimaryResource: sdk.ObjectRef{Kind: "Pod", APIVersion: "v1", Namespace: "app", Name: "worker"}, Evidence: "synthetic", Summary: "unready"}}
	cfg := testControllerConfig()
	cfg.ClosureStabilityWindow = 5 * time.Second
	c, err := NewController(cfg, []sdk.Detector{world}, staticProvider{}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	c.now = clock.Now
	c.currentIncidentRequest = &IncidentRequest{IncidentID: "synthetic", Application: "demo", Findings: findings,
		OpeningKeys: findingKeys(findings), ResponseDeadline: now.Add(time.Minute)}
	c.incidentOpen = true
	c.incidentDetectedAt, c.incidentDispatchedAt = now, now
	result := completedResult("synthetic")
	c.handleDispatchCompletion(dispatchCompletion{incidentID: result.IncidentID, result: result}, now)
	var closed time.Time
	c.OnIncidentClosed = func(closure IncidentClosure) { closed = closure.VerifiedAt }
	autoStep(t, c, clock, time.Second)
	autoStep(t, c, clock, 4*time.Second)
	if !closed.IsZero() {
		t.Fatal("direct responder incident closed before its existing stability window")
	}
	autoStep(t, c, clock, time.Second)
	if want := now.Add(6 * time.Second); !closed.Equal(want) {
		t.Fatalf("direct responder incident closed at %s, want %s", closed, want)
	}
}

func TestAutomaticRecoveryWaitRequiresALiveWrite(t *testing.T) {
	for name, action := range map[string]*contracts.Action{
		"wait":             {Kind: &contracts.Action_WaitFor{}},
		"credential check": {Kind: &contracts.Action_CheckStoreCredential{}},
		"backup":           {Kind: &contracts.Action_StoreBackup{}},
		"source proposal":  {Kind: &contracts.Action_ProposeSourceChange{}},
	} {
		t.Run(name, func(t *testing.T) {
			receipt := &contracts.PlanApplication{Applied: true, ActionsApplied: []*contracts.Action{action}}
			if stageReceiptWrote(receipt) {
				t.Fatalf("the executor completed %s without a live write", name)
			}
		})
	}
}

func TestAutomaticRecoveryClosesWithAJoinedPage(t *testing.T) {
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 12 * time.Second}, settings.AfterPlaybookAgent)
	if err := c.admitPage(t.Context(), world.clock.Now(), nil, ExternalIncident{
		ID: "fallback-page", Namespace: "app", Message: "Users still see failures",
	}); err != nil {
		t.Fatal(err)
	}
	autoStep(t, c, world.clock, 12*time.Second)
	autoStep(t, c, world.clock, 5*time.Second)
	autoStep(t, c, world.clock, time.Second)
	report := deterministicStageReport(closed.Request.PriorStages)
	if c.IncidentOpen() || closed.Request.Page == "" || !closed.EscalatedAt.IsZero() ||
		report.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
		t.Fatalf("joined page held automatic recovery: closure=%v report=%v", closed.VerifiedAt, report)
	}
}

type delayedAutomaticSnapshot struct {
	world *settlingWorld
	err   error
}

func (p delayedAutomaticSnapshot) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	lag, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	<-lag.Done()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := p.world.clock.advance(ctx, 2*time.Second); err != nil {
		return nil, err
	}
	if p.err != nil {
		return nil, p.err
	}
	return p.world.Snapshot(ctx)
}

func TestAutomaticRecoveryUsesClearSampleThatCrossesDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 94 * time.Second}, settings.AfterPlaybookAgent)
		c.provider = delayedAutomaticSnapshot{world: world}
		bound := autoReport(c).GetRecoveryDeadline().AsTime()
		autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
		report := autoReport(c)
		if !report.GetStabilityDeadline().IsValid() || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
			t.Fatalf("clear sample crossing the deadline did not enter stability: report=%v escalated=%v", report, c.escalatedAt)
		}
		c.provider = world
		for range 4 {
			autoStep(t, c, world.clock, time.Second)
		}
		if closed.Result == nil || !closed.EscalatedAt.IsZero() {
			t.Fatalf("stability did not close the recovered incident: stable since %v now %v report %v", c.closureStableSince, world.clock.Now(), report)
		}
	})
}

func TestAutomaticDeadlineHandsOffAfterSlowOrUnavailableObservation(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"slow firing sample", nil},
		{"unavailable sample", sdk.ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
				c.provider = delayedAutomaticSnapshot{world: world, err: tc.err}
				bound := autoReport(c).GetRecoveryDeadline().AsTime()
				autoStep(t, c, world.clock, bound.Sub(world.clock.Now()))
				if tc.err == nil {
					states, _ := c.latestEvaluations([]string{"traffic-health"}, bound, false)
					if len(states) != 1 || states[0].Status != DetectorEvaluationFiring || !states[0].EvaluatedAt.Equal(bound) {
						t.Fatalf("slow firing sample lost its step timestamp: %v", states)
					}
				}
				report := autoReport(c)
				if !report.GetHandoffSelected() || !slices.Equal(report.GetHandoffKeys(), c.currentIncidentRequest.OpeningKeys) ||
					!c.escalatedAt.IsZero() || c.awaitingAutomaticRecoveryLocked() != nil {
					t.Fatalf("deadline did not select one handoff for the alert set: report=%v escalated=%v", report, c.escalatedAt)
				}
			})
		})
	}
}

func TestAutomaticStageLeavesAPageOpenedIncidentForTheResponder(t *testing.T) {
	world := newSettlingWorld(settlingCase{})
	stage, state := settlingStageOn(t, settlingCase{}, world)
	state.Request.Page = "Investigate an externally reported symptom"
	report, err := stage.Run(t.Context(), state)
	if err != nil || playbookWrote(report) || world.restarts.Load() != 0 {
		t.Fatalf("automatic stage acted on page clues: report=%v err=%v", report, err)
	}
}

type unavailableHandoffView struct {
	reads int
}

func (p *unavailableHandoffView) Snapshot(context.Context) (sdk.DetectionContext, error) {
	p.reads++
	return nil, sdk.ErrUnavailable
}

type preparingAutomaticHandoff struct {
	recordingDispatcher
	loop *ResponderLoop
}

func (d *preparingAutomaticHandoff) PrepareDispatch(ctx context.Context, request IncidentRequest) (IncidentRequest, error) {
	request = d.loop.prepareDispatch(ctx, request, nil, goCodePathCoverage{})
	request.PreparedAgentTask = "synthetic-frozen-task"
	return request, nil
}

type unavailableHandoffClue struct{ closureScopeDetector }

func (d *unavailableHandoffClue) Spec() sdk.DetectorSpec {
	spec := d.closureScopeDetector.Spec()
	spec.Class, spec.Owner = sdk.DetectorClassLearned, sdk.DetectorOwnerResponder
	spec.OriginatingIncident = "synthetic-prior-incident"
	return spec
}

func TestAutomaticHandoffAndClosureIgnoreAPermanentlyUnavailableClue(t *testing.T) {
	clue := &unavailableHandoffClue{closureScopeDetector{id: "optional-clue", unavailable: true}}
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: 100 * time.Second}, settings.AfterPlaybookAgent, clue)
	loop := c.responderLoop
	loop.Detectors = append(loop.Detectors, clue)
	dispatcher := &preparingAutomaticHandoff{recordingDispatcher: recordingDispatcher{requests: make(chan IncidentRequest, 2)}, loop: loop}
	c.dispatcher = dispatcher
	step := func(advance time.Duration) {
		t.Helper()
		if err := world.clock.advance(t.Context(), advance); err != nil {
			t.Fatal(err)
		}
		if err := c.Step(t.Context(), world.clock.Now(), nil); err != nil {
			t.Fatal(err)
		}
		c.Wait()
	}

	bound := autoReport(c).GetRecoveryDeadline().AsTime()
	step(bound.Sub(world.clock.Now()))
	if !autoReport(c).GetHandoffSelected() {
		t.Fatal("the still-firing alert did not select handoff")
	}
	unavailable := new(unavailableHandoffView)
	loop.Provider = unavailable
	if prepared, err := c.PrepareDispatchStep(t.Context()); err != nil || !prepared {
		t.Fatalf("prepare handoff = %v, %v", prepared, err)
	}
	loop.Provider = world
	effect, ok := c.PendingDispatchEffect()
	if !ok {
		t.Fatal("handoff did not publish a pending dispatch")
	}
	if err := c.ExecuteDispatchEffect(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	step(time.Second)
	step(4 * time.Second)
	step(5 * time.Second)
	step(time.Second)
	samples, _ := c.latestEvaluations([]string{clue.id}, bound, false)
	if len(samples) != 1 || samples[0].Status != DetectorEvaluationError {
		t.Fatalf("test did not exercise a permanently unavailable clue: %v", samples)
	}
	if dispatcher.callCount() != 1 || c.IncidentOpen() || closed.Result == nil || !closed.EscalatedAt.IsZero() {
		t.Fatalf("claims=%d open=%v closure=%s escalated=%s", dispatcher.callCount(), c.IncidentOpen(), closed.VerifiedAt, closed.EscalatedAt)
	}
	if len(closed.FinalDetectorStates) != 1 || closed.FinalDetectorStates[0].DetectorID != "traffic-health" {
		t.Fatalf("recovery used observations outside the alert set: %v", closed.FinalDetectorStates)
	}
	if unavailable.reads != 0 {
		t.Fatalf("handoff preparation made %d live observation reads", unavailable.reads)
	}
}

type handoffRecoveryProbe struct {
	windowedSettlingDetector
	repaired atomic.Bool
}

func (d *handoffRecoveryProbe) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.repaired.Load() {
		return nil, nil
	}
	return d.windowedSettlingDetector.Detect(ctx, view)
}

// This responder fixes the synthetic fault on request and withholds its
// report while the controller verifies recovery.
type recoveryWaitingHandoff struct {
	probe    *handoffRecoveryProbe
	started  chan struct{}
	repair   chan struct{}
	repaired chan struct{}
}

func (d *recoveryWaitingHandoff) Dispatch(ctx context.Context, _ IncidentRequest) (IncidentResult, error) {
	close(d.started)
	select {
	case <-d.repair:
		d.probe.repaired.Store(true)
		close(d.repaired)
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	<-ctx.Done()
	return IncidentResult{}, ctx.Err()
}

func TestAutomaticHandoffVerifiesRecoveryWhileResponderWaits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		applied     bool
		unencodable bool
	}{
		{name: "applied", applied: true},
		{name: "nothing applied"},
		{name: "unencodable responder application", unencodable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{
				unexplainedDrift: true, undecided: !tc.applied,
			}, settings.AfterPlaybookAgent)
			ctx, cancel := context.WithCancel(t.Context())
			defer func() { cancel(); c.Wait() }()
			report := autoReport(c)
			if playbookWrote(report) != tc.applied {
				t.Fatalf("automatic live write = %v, want %v", playbookWrote(report), tc.applied)
			}
			unresolved := c.currentIncidentResult.UnresolvedFindings
			clueIndex := slices.IndexFunc(unresolved, func(f sdk.Finding) bool {
				return f.DetectorID == "drift.unexplained" && f.Status == sdk.FindingActive
			})
			if clueIndex < 0 {
				t.Fatal("automatic stage finished without a residual drift clue")
			}
			clue := unresolved[clueIndex]
			c.tracker.ObserveBinding(world.clock.Now(), clue, true)

			probe := &handoffRecoveryProbe{windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector)}
			c.detectors[probe.id] = probe
			c.responderLoop.Detectors[0] = probe
			c.scheduler = NewScheduler([]sdk.Detector{probe}, world.clock.Now())
			dispatcher := &recoveryWaitingHandoff{probe: probe, started: make(chan struct{}), repair: make(chan struct{}), repaired: make(chan struct{})}
			c.dispatcher = dispatcher
			stepAt := func(at time.Time) {
				t.Helper()
				if err := world.clock.advance(ctx, at.Sub(world.clock.Now())); err != nil {
					t.Fatal(err)
				}
				if err := c.Step(ctx, at, nil); err != nil {
					t.Fatal(err)
				}
			}
			// A stage that wrote nothing handed off when it ended, so its deadline is now.
			at := world.clock.Now()
			if tc.applied {
				at = c.automaticDeadlineLocked(report)
			}
			stepAt(at)
			effect, ok := c.PendingDispatchEffect()
			if !ok || !autoReport(c).GetHandoffSelected() {
				t.Fatal("the residual alert did not hand off to the responder")
			}
			if err := c.ExecuteDispatchEffect(ctx, effect); err != nil {
				t.Fatal(err)
			}
			<-dispatcher.started
			close(dispatcher.repair)
			<-dispatcher.repaired

			if tc.unencodable {
				action := program.SetField{
					Ref:  sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "synthetic"},
					Path: "data.key",
				}
				c.responderLoop.mu.Lock()
				c.responderLoop.applicationsLog = append(c.responderLoop.applicationsLog, RecordedPlanApplication{
					ID: "synthetic-unencodable", Result: PlanApplicationResult{
						Plan: program.Plan{Actions: []program.Action{action}}, ActionsApplied: []program.Action{action}, Applied: true,
					},
				})
				c.responderLoop.mu.Unlock()
			}

			for i := 0; c.IncidentOpen() && i < 12; i++ {
				wake := c.NextWake()
				if !wake.After(world.clock.Now()) {
					t.Fatalf("open incident has no future wake: %s", wake)
				}
				stepAt(wake)
				if i == 0 && c.closureStableSince.IsZero() {
					t.Fatal("controller never started recovery verification while the responder was waiting")
				}
			}
			if c.IncidentOpen() || !closed.VerifiedWhileResponderRunning || closed.HealthClearedAt == nil {
				t.Fatalf("controller did not verify and close while the responder waited: %+v", closed)
			}
			if tc.unencodable {
				// Derive from the restored closure to exercise the durable round trip.
				derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, *c.pendingClosure, "", "synthetic", "synthetic")
				if err != nil {
					t.Fatal(err)
				}
				index := slices.IndexFunc(derived.Record.GetPlanApplications(), func(app *contracts.PlanApplication) bool {
					return app.GetApplicationId() == "synthetic-unencodable"
				})
				if index < 0 {
					t.Fatal("outcome omitted the unencodable application")
				}
				actions := derived.Record.GetPlanApplications()[index].GetActionsApplied()
				if len(actions) != 1 {
					t.Fatalf("applied actions = %v", actions)
				}
				gap := actions[0].GetUnencodable()
				if gap.GetActionType() != "program.SetField" || gap.GetRef().GetName() != "synthetic" ||
					gap.GetReason() != "unencodable: action_encoding" {
					t.Fatalf("outcome placeholder = %v", gap)
				}
			}

			if !c.tracker.HasActiveKeys([]string{findingKey(clue)}) {
				t.Fatal("the drift clue cleared during the simulation")
			}
			if len(closed.FinalDetectorStates) != 1 || closed.FinalDetectorStates[0].DetectorID != probe.id {
				t.Fatalf("expected scoped recovery despite the remaining clue: %v", closed.FinalDetectorStates)
			}
		})
	}
}

func TestAutomaticRecoveryExtendsWhenClearAtDeadline(t *testing.T) {
	for _, name := range []string{"recovers", "refires", "unsettled"} {
		t.Run(name, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
			probe := &handoffRecoveryProbe{windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector)}
			c.detectors[probe.id] = probe
			report := autoReport(c)
			bound := report.GetRecoveryDeadline().AsTime()
			stepSeconds(t, c, world, 10) // the writes settle, then one unsettles late
			setUpdatedReplicas(t, world, 0)
			autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-400*time.Millisecond)
			probe.repaired.Store(true)
			autoStep(t, c, world.clock, 400*time.Millisecond)
			extension := bound.Add(c.config.ClosureStabilityWindow + probe.Spec().Interval)
			if got := report.GetStabilityDeadline().AsTime(); !got.Equal(extension) ||
				!c.IncidentOpen() || !c.escalatedAt.IsZero() || report.GetHandoffSelected() {
				t.Fatalf("clear alerts at stage bound: stability deadline=%v, want %v, escalated=%v, handoff=%v",
					report.GetStabilityDeadline(), extension, c.escalatedAt, report.GetHandoffSelected())
			}

			switch name {
			case "recovers":
				setUpdatedReplicas(t, world, 1)
				for i := 0; c.IncidentOpen() && i < 12; i++ {
					autoStep(t, c, world.clock, time.Second)
				}
				if c.IncidentOpen() || closed.Result == nil || !closed.EscalatedAt.IsZero() ||
					deterministicStageReport(closed.Request.PriorStages).GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
					t.Fatalf("recovery did not close the incident: %+v", closed)
				}
			case "refires":
				probe.repaired.Store(false)
				autoStep(t, c, world.clock, time.Second)
				if !c.IncidentOpen() || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
					t.Fatal("recurrence interrupted the extension")
				}
				autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
				if !c.IncidentOpen() || !report.GetHandoffSelected() || !c.escalatedAt.IsZero() ||
					!slices.Equal(report.GetHandoffKeys(), c.currentIncidentRequest.OpeningKeys) {
					t.Fatalf("recurrence did not hand off the open incident's alert: %v", report)
				}
			case "unsettled":
				autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
				if !c.escalatedAt.Equal(extension) || report.GetHandoffSelected() ||
					!report.GetMitigations()[0].GetRolloutUnsettledAtDeadline() {
					t.Fatalf("unsettled rollout at expiry: escalated=%v, report=%v", c.escalatedAt, report)
				}
				autoStep(t, c, world.clock, time.Second)
				if !c.IncidentOpen() || !c.escalatedAt.Equal(extension) || !report.GetStabilityDeadline().AsTime().Equal(extension) {
					t.Fatal("unsettled rollout changed the escalation after the single extension")
				}
			}
		})
	}
}

func TestClosureFreezeBackoffDoesNotDelayAutomaticHandoff(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	bound := autoReport(c).GetRecoveryDeadline().AsTime()
	// A closure freeze failed before the alert re-fired. Its retry may be
	// after the response deadline, so waiting for it could skip handoff.
	c.closureFreezeRetryAt = c.currentIncidentRequest.ResponseDeadline.Add(time.Second)
	autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
	if autoReport(c).GetHandoffSelected() {
		t.Fatal("handoff selected before the recovery deadline")
	}
	// Remove the periodic detector wake so this assertion exercises the
	// automatic deadline itself. Recovery still uses the detector registry.
	c.scheduler = NewScheduler(nil, world.clock.Now(), c.config.Namespace)
	if wake, reason := c.nextWake(); !wake.Equal(bound) || reason != "verification deadline" {
		t.Fatalf("next wake %s (%s), want the automatic recovery deadline %s", wake, reason, bound)
	}
	autoStep(t, c, world.clock, time.Second)
	if !autoReport(c).GetHandoffSelected() ||
		c.responseEntry(*c.currentIncidentRequest) != dispatchStagedHandoff {
		t.Fatal("closure freeze backoff prevented automatic handoff at its deadline")
	}
}

// A stage that wrote nothing leaves the agent's decision to the frozen request:
// the handoff is selected when the stage ends, with no further observation.
func TestStageThatWroteNothingHandsOffWithoutAnObservation(t *testing.T) {
	c, _, _ := automaticRecoveryWorld(t, settlingCase{undecided: true}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	if playbookWrote(report) {
		t.Fatal("the synthetic stage wrote")
	}
	if !report.GetHandoffSelected() || len(report.GetHandoffKeys()) == 0 || c.responderDone ||
		c.dispatchState != contracts.DispatchState_DISPATCH_STATE_PENDING ||
		c.responseEntry(*c.currentIncidentRequest) != dispatchStagedHandoff {
		t.Fatalf("no handoff when the stage ended: %+v, dispatch %s", report, c.dispatchState)
	}
}

// Evidence queued while the stage ran still gets its automatic pass first.
func TestStageThatWroteNothingWaitsForItsQueuedPass(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	c, _ := churnController(t, now)
	stage := &contracts.StageReport{Stage: "deterministic",
		RecoveryWait: &contracts.RecoveryWait{ExitReason: contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED}}
	c.currentIncidentRequest.PriorStages = nil
	c.responderDone = false
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
	c.responseStagesRunning = true
	c.admitActivation(t.Context(), routeErrors(t, "route-fix"), now, sdk.DetectorSpec{})
	id := c.currentIncidentRequest.IncidentID
	c.results <- dispatchCompletion{incidentID: id, priorStages: []*contracts.StageReport{stage},
		result: IncidentResult{IncidentID: id, Status: IncidentCompleted, ResolutionPath: "deterministic"}}
	if !c.takeReadyDispatchWork(now) || c.takeReadyDispatchWork(now) {
		t.Fatal("the finished stage was not taken exactly once")
	}
	if autoReport(c).GetHandoffSelected() || !c.pendingLearnedPassReadyLocked() {
		t.Fatal("handed off before the queued automatic pass")
	}
}

// However slowly the alert set's detectors clear and its workloads start, the
// estimate after the last write is at most the configured wait bound
// (response.after_playbook_wait), so a slow probe or a long startup history
// never holds the agent past it.
func TestTheRecoveryEstimateIsCappedByTheWaitBound(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{startup: 15 * time.Minute}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	written := lastStageReceipt(report)
	if got, want := report.GetRecoveryDeadline().AsTime(), written.Add(c.config.AfterPlaybookWait); !got.Equal(want) {
		t.Fatalf("deadline = %s after the write, want the %s bound", got.Sub(written), c.config.AfterPlaybookWait)
	}
	view, err := world.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	slow := windowedSettlingDetector{settlingDetector{id: "traffic-health", window: 12 * time.Minute}}
	if got := automaticRecoveryDeadline(*c.currentIncidentRequest, report, written, view, []sdk.Detector{slow}, 0, 5*time.Minute, 5*time.Second); !got.Equal(written.Add(5 * time.Minute)) {
		t.Fatalf("deadline = %s after the write, want 5m", got.Sub(written))
	}
}
