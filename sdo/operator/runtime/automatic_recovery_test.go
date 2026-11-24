package runtime

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	outcomeclass "sdo.dev/operator/contracts/outcome"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
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
	stage.StabilityWindow = cfg.ClosureStabilityWindow
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
	if !extension.Equal(world.clock.Now().Add(6*time.Second)) || !report.GetRecoveryDeadline().AsTime().Equal(initial) {
		t.Fatalf("deadline and extension = %s, %s", report.GetRecoveryDeadline(), report.GetStabilityDeadline())
	}
	if report.GetHandoffSelected() || !c.escalatedAt.IsZero() || report.GetMitigations()[0].GetRolloutUnsettledAtDeadline() {
		t.Fatal("a stability-only wait handed off, escalated or debited trust")
	}
	autoStep(t, c, world.clock, 3*time.Second)
	autoStep(t, c, world.clock, time.Second)
	if closed.Result != nil {
		t.Fatal("recovery shortened the fixed extension")
	}
	autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
	if c.IncidentOpen() {
		autoStep(t, c, world.clock, 0)
	}
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
		failing bool
		handoff bool
	}{
		{"one still firing", settings.AfterPlaybookAgent, true, true},
		{"escalate policy", settings.AfterPlaybookEscalate, true, false},
		{"no firing, rollout unsettled", settings.AfterPlaybookAgent, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := settlingCase{}
			if !tc.failing {
				data.healAfter = 15 * time.Second
			}
			c, world, _ := automaticRecoveryWorld(t, data, tc.policy)
			bound := autoReport(c).GetRecoveryDeadline().AsTime()
			if !tc.failing {
				ref := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
				setRolloutUnsettled(t, world, ref)
			}
			autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
			if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
				t.Fatal("handoff or escalation before the fixed deadline")
			}
			autoStep(t, c, world.clock, time.Second)
			report := autoReport(c)
			if !tc.failing {
				extension := report.GetStabilityDeadline().AsTime()
				autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
				if report.GetHandoffSelected() || !c.escalatedAt.IsZero() || report.GetMitigations()[0].GetRolloutUnsettledAtDeadline() {
					t.Fatal("a clear alert escalated while settlement was pending")
				}
				if err := c.Step(t.Context(), world.clock.Now(), nil); err != nil {
					t.Fatal(err)
				}
				if !c.NextWake().After(world.clock.Now()) {
					t.Fatal("expired extension caused an immediate wake with clear alerts")
				}
				return
			}
			if report.GetHandoffSelected() != tc.handoff || c.escalatedAt.IsZero() == !tc.handoff {
				t.Fatalf("handoff = %v, escalated at %s, want handoff = %v", report.GetHandoffSelected(), c.escalatedAt, tc.handoff)
			}
		})
	}
}

func setRolloutUnsettled(t *testing.T, world *settlingWorld, ref sdk.ObjectRef) {
	t.Helper()
	deployments := world.client.AppsV1().Deployments(ref.Namespace)
	object, err := deployments.Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Status.UpdatedReplicas = 0
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
	got := automaticRecoveryDeadline(request, report, now, view, []sdk.Detector{long}, 0, 5*time.Second)
	if want := now.Add(166 * time.Second); !got.Equal(want) {
		t.Fatalf("deadline with long clearing lag = %s, want %s", got, want)
	}
	request.ResponseDeadline = now.Add(time.Minute)
	if got := automaticRecoveryDeadline(request, report, now, view, []sdk.Detector{long}, 4*time.Minute, 5*time.Second); !got.Equal(request.ResponseDeadline) {
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
		extension := report.GetStabilityDeadline().AsTime()
		autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
		if c.IncidentOpen() {
			autoStep(t, c, world.clock, 0)
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
				if tc.err != nil {
					extension := report.GetStabilityDeadline().AsTime()
					if !extension.After(world.clock.Now()) || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
						t.Fatalf("unavailable snapshot did not wait for verification: %v", report)
					}
					autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
				}
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
		name    string
		applied bool
	}{
		{"applied", true},
		{"nothing applied", false},
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
			stepAt(c.automaticDeadlineLocked(report))
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
			if !c.tracker.HasActiveKeys([]string{findingKey(clue)}) {
				t.Fatal("the drift clue cleared during the simulation")
			}
			if len(closed.FinalDetectorStates) != 1 || closed.FinalDetectorStates[0].DetectorID != probe.id {
				t.Fatalf("expected scoped recovery despite the remaining clue: %v", closed.FinalDetectorStates)
			}
		})
	}
}

type deadlineConfirmationProbe struct {
	windowedSettlingDetector
	confirmAt   time.Time
	unavailable bool
}

func (d *deadlineConfirmationProbe) Confirmed(sdk.DetectionContext) bool {
	return !d.clock.Now().Before(d.confirmAt)
}

func (d *deadlineConfirmationProbe) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.unavailable {
		return nil, sdk.ErrUnavailable
	}
	return d.windowedSettlingDetector.Detect(ctx, view)
}

func TestAutomaticClearAtHandoffDeadline(t *testing.T) {
	for _, scenario := range []string{"393ms before deadline, unconfirmed", "confirmed exactly at deadline", "refires during extension", "detector unavailable", "clear at response cap"} {
		t.Run(scenario, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
			report := autoReport(c)
			bound := report.GetRecoveryDeadline().AsTime()
			probe := &deadlineConfirmationProbe{
				windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector),
				confirmAt:                bound,
			}
			probe.healAt = bound
			switch scenario {
			case "393ms before deadline, unconfirmed":
				probe.healAt = bound.Add(-393 * time.Millisecond)
				probe.confirmAt = bound.Add(time.Second)
			case "detector unavailable":
				probe.unavailable = true
			case "clear at response cap":
				c.currentIncidentRequest.ResponseDeadline = bound.Add(time.Second)
				probe.confirmAt = c.currentIncidentRequest.ResponseDeadline
			}
			c.detectors[probe.id] = probe
			c.responderLoop.Detectors[0] = probe
			autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
			autoStep(t, c, world.clock, time.Second)

			extension := report.GetStabilityDeadline().AsTime()
			wantExtension := bound.Add(probe.Spec().Interval + c.config.ClosureStabilityWindow)
			if cap := c.currentIncidentRequest.ResponseDeadline; cap.Before(wantExtension) {
				wantExtension = cap
			}
			if !extension.Equal(wantExtension) || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
				t.Fatalf("initial deadline: extension=%s want=%s, handoff=%v escalation=%s", extension, wantExtension, report.GetHandoffSelected(), c.escalatedAt)
			}
			if scenario == "refires during extension" {
				probe.healAt = time.Time{}
			}
			autoStep(t, c, world.clock, time.Second)
			if world.clock.Now().Before(extension) && (report.GetHandoffSelected() || !c.escalatedAt.IsZero()) {
				t.Fatal("extension ended early")
			}
			autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
			if !report.GetRecoveryDeadline().AsTime().Equal(bound) || !report.GetStabilityDeadline().AsTime().Equal(extension) || world.restarts.Load() != 1 {
				t.Fatal("verification moved a deadline or ran another repair")
			}

			if scenario == "refires during extension" || scenario == "detector unavailable" {
				if !report.GetHandoffSelected() || !slices.Equal(report.GetHandoffKeys(), c.currentIncidentRequest.OpeningKeys) || !c.escalatedAt.IsZero() {
					t.Fatalf("extension did not select one scoped handoff: %v, escalation=%s", report, c.escalatedAt)
				}
				return
			}
			if c.IncidentOpen() {
				autoStep(t, c, world.clock, c.config.ClosureStabilityWindow)
			}
			if c.IncidentOpen() {
				autoStep(t, c, world.clock, time.Second)
			}
			closedReport := deterministicStageReport(closed.Request.PriorStages)
			if closed.Result == nil || closed.HealthClearedAt == nil || !closed.EscalatedAt.IsZero() ||
				closedReport.GetHandoffSelected() || closedReport.GetRecoveryWait().GetExitReason() != contracts.RecoveryWait_EXIT_REASON_CLEARED {
				t.Fatalf("expected stage-only verified closure, got %v, report %v", closed.VerifiedAt, closedReport)
			}
			outcome, err := (ProtoOutcomeDeriver{}).Derive(t.Context(), *closed, "", "synthetic", "synthetic")
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Record.GetClassification() != outcomeclass.Success {
				t.Fatalf("stage-only repair outcome = %s", outcome.Record.GetClassification())
			}
			id := report.GetMitigations()[0].GetPlaybookId()
			if trust := ProgramTrustStats([]*contracts.OutcomeRecord{outcome.Record})[id]; trust.Failures != 0 {
				t.Fatalf("recovered stage charged failure: %+v", trust)
			}
		})
	}
}

func TestAlertJoiningDuringAutomaticExtensionKeepsTheDeadline(t *testing.T) {
	probe := &switchedProbe{id: "traffic-late", scenario: "late"}
	probe.failing.Store(true)
	c, world, _ := automaticRecoveryWorld(t, settlingCase{healAfter: 95 * time.Second}, settings.AfterPlaybookAgent, probe)
	clock := world.clock
	deadline := autoReport(c).GetRecoveryDeadline().AsTime()
	autoStep(t, c, clock, deadline.Sub(clock.Now()))
	extension := autoReport(c).GetStabilityDeadline().AsTime()
	if !extension.After(deadline) || autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
		t.Fatal("clear opening alert did not enter verification")
	}

	_ = clock.advance(t.Context(), time.Second)
	if !c.joinAlertSet(probe.finding(), clock.Now()) {
		t.Fatal("late alert never joined")
	}
	autoStep(t, c, clock, 0)
	if autoReport(c).GetHandoffSelected() || !c.escalatedAt.IsZero() {
		t.Fatal("the join ended the extension before its bound")
	}

	checkpoint, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	detectors := []sdk.Detector{c.detectors["traffic-health"], probe}
	restarted, err := NewController(c.config, detectors, c.provider, c.dispatcher, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.now, restarted.codePath = clock.Now, c.codePath
	if err := restarted.RestoreState(checkpoint); err != nil {
		t.Fatal(err)
	}
	autoStep(t, restarted, clock, extension.Sub(clock.Now()))
	report := autoReport(restarted)
	if !report.GetStabilityDeadline().AsTime().Equal(extension) || !report.GetHandoffSelected() ||
		!slices.Equal(report.GetHandoffKeys(), []string{findingKey(probe.finding())}) || !restarted.escalatedAt.IsZero() {
		t.Fatalf("late alert failed to hand off at the restored deadline: %v", report)
	}
}

func TestAutomaticExtensionWaitsDespiteLaterStageEscalation(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	deadline := report.GetRecoveryDeadline().AsTime()
	probe := &deadlineConfirmationProbe{
		windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector),
		unavailable:              true,
	}
	c.detectors[probe.id] = probe
	c.responderLoop.Detectors[0] = probe
	report.Mitigations = append(report.Mitigations, &contracts.StageMitigation{PlaybookId: "later", Result: stageEscalated})
	autoStep(t, c, world.clock, deadline.Sub(world.clock.Now()))
	end := report.GetStabilityDeadline().AsTime()
	if !end.After(deadline) {
		t.Fatal("stage escalation did not allow a verification extension")
	}
	autoStep(t, c, world.clock, time.Second)
	if !c.escalatedAt.IsZero() || report.GetHandoffSelected() || !report.GetStabilityDeadline().AsTime().Equal(end) {
		t.Fatal("later stage escalation bypassed the fixed extension")
	}
	autoStep(t, c, world.clock, end.Sub(world.clock.Now()))
	if !c.escalatedAt.Equal(end) || report.GetHandoffSelected() {
		t.Fatal("out-of-reach stage attempt did not retain its escalation policy at expiry")
	}
}

func TestAutomaticExtensionConsumedDespiteLaterUnavailableRead(t *testing.T) {
	c, world, _ := automaticRecoveryWorld(t, settlingCase{healAfter: time.Second}, settings.AfterPlaybookAgent)
	report := autoReport(c)
	deadline := report.GetRecoveryDeadline().AsTime()
	probe := &deadlineConfirmationProbe{windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector)}
	c.detectors[probe.id] = probe
	c.responderLoop.Detectors[0] = probe
	setRolloutUnsettled(t, world, sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"})
	autoStep(t, c, world.clock, deadline.Sub(world.clock.Now()))
	end := report.GetStabilityDeadline().AsTime()
	autoStep(t, c, world.clock, end.Sub(world.clock.Now()))
	if report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
		t.Fatal("clear alerts at the extension end transferred ownership")
	}
	probe.unavailable = true
	autoStep(t, c, world.clock, time.Second)
	if report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
		t.Fatal("a failed read after the extension end transferred ownership")
	}

	state, err := c.ExportState()
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewController(c.config, []sdk.Detector{probe}, world, c.dispatcher, world.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	restarted.now, restarted.codePath = world.clock.Now, c.codePath
	if err := restarted.RestoreState(state); err != nil {
		t.Fatal(err)
	}
	autoStep(t, restarted, world.clock, time.Second)
	if autoReport(restarted).GetHandoffSelected() || !restarted.escalatedAt.IsZero() {
		t.Fatal("a restart reopened the extension decision")
	}
}

func TestAutomaticResponseDeadlineRequiresFreshClear(t *testing.T) {
	for _, tc := range []struct {
		name        string
		unavailable bool
		wantStatus  string
	}{
		{name: "refire after extension", wantStatus: "firing"},
		{name: "unavailable at deadline", unavailable: true, wantStatus: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: time.Second}, settings.AfterPlaybookAgent)
			probe := &deadlineConfirmationProbe{windowedSettlingDetector: c.detectors["traffic-health"].(windowedSettlingDetector)}
			c.detectors[probe.id] = probe
			c.responderLoop.Detectors[0] = probe
			report := autoReport(c)
			setRolloutUnsettled(t, world, sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"})
			bound := c.currentIncidentRequest.ResponseDeadline
			key := c.currentIncidentRequest.OpeningKeys[0]
			autoStep(t, c, world.clock, report.GetRecoveryDeadline().AsTime().Sub(world.clock.Now()))
			extension := report.GetStabilityDeadline().AsTime()
			autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
			if !report.GetStabilityDeadlineEvaluated() || report.GetHandoffSelected() || !c.escalatedAt.IsZero() {
				t.Fatal("clear alerts at extension expiry did not consume the handoff decision")
			}

			if !tc.unavailable {
				probe.healAt = time.Time{}
			}
			autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Second)
			if !c.IncidentOpen() || !c.escalatedAt.IsZero() || report.GetHandoffSelected() {
				t.Fatal("late probe changed the stage decision before the response deadline")
			}
			probe.unavailable = tc.unavailable
			autoStep(t, c, world.clock, time.Second)
			if !c.IncidentOpen() || closed.Result != nil || !c.escalatedAt.Equal(bound) {
				t.Fatalf("response deadline must escalate in place: open=%v escalated=%s closure=%q", c.IncidentOpen(), c.escalatedAt, closed.UnconfirmedDetail)
			}
			if report.GetHandoffSelected() || !report.GetStabilityDeadline().AsTime().Equal(extension) {
				t.Fatal("response deadline reopened the handoff decision")
			}
			wantReason := tc.wantStatus + "=[" + strconv.Quote(key) + "]"
			if !strings.Contains(report.GetError(), wantReason) {
				t.Fatalf("escalation reason = %q, want %q", report.GetError(), wantReason)
			}
			autoStep(t, c, world.clock, time.Second)
			if !c.IncidentOpen() {
				t.Fatal("escalated incident closed while an alert was still unresolved")
			}
		})
	}
}

func TestAutomaticClearAndSettledWriteAtResponseDeadline(t *testing.T) {
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: time.Second}, settings.AfterPlaybookAgent)
	ref := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}
	setRolloutUnsettled(t, world, ref)
	report := autoReport(c)
	autoStep(t, c, world.clock, report.GetRecoveryDeadline().AsTime().Sub(world.clock.Now()))
	extension := report.GetStabilityDeadline().AsTime()
	autoStep(t, c, world.clock, extension.Sub(world.clock.Now()))
	if !report.GetStabilityDeadlineEvaluated() || !c.IncidentOpen() || !c.escalatedAt.IsZero() {
		t.Fatal("clear alerts with a pending write did not enter the settlement wait")
	}

	deployments := world.client.AppsV1().Deployments(ref.Namespace)
	object, err := deployments.Get(t.Context(), ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	object.Status.UpdatedReplicas = 1
	if _, err := deployments.UpdateStatus(t.Context(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	autoStep(t, c, world.clock, c.currentIncidentRequest.ResponseDeadline.Sub(world.clock.Now()))
	if !c.IncidentOpen() || !c.escalatedAt.IsZero() || report.GetHandoffSelected() {
		t.Fatal("settling at the response deadline must allow the stability window")
	}
	autoStep(t, c, world.clock, c.config.ClosureStabilityWindow)
	if c.IncidentOpen() {
		autoStep(t, c, world.clock, 0)
	}
	if c.IncidentOpen() || !closed.EscalatedAt.IsZero() || closed.HealthClearedAt == nil {
		t.Fatalf("clear settled incident did not close as verified: open=%v escalated=%s detail=%q", c.IncidentOpen(), closed.EscalatedAt, closed.UnconfirmedDetail)
	}
}

func TestAutomaticUnsettledWriteEndsAtResponseDeadline(t *testing.T) {
	c, world, closed := automaticRecoveryWorld(t, settlingCase{healAfter: time.Second}, settings.AfterPlaybookAgent)
	setRolloutUnsettled(t, world, sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"})
	deadline := autoReport(c).GetRecoveryDeadline().AsTime()
	autoStep(t, c, world.clock, deadline.Sub(world.clock.Now()))
	end := autoReport(c).GetStabilityDeadline().AsTime()
	autoStep(t, c, world.clock, end.Sub(world.clock.Now()))
	bound := c.currentIncidentRequest.ResponseDeadline
	c.scheduler = NewScheduler(nil, end)
	if got := c.NextWake(); !got.Equal(bound) {
		t.Errorf("wake after spent extension = %s, want response deadline %s", got, bound)
	}
	autoStep(t, c, world.clock, bound.Sub(world.clock.Now())-time.Millisecond)
	if !c.IncidentOpen() || !c.escalatedAt.IsZero() {
		t.Fatal("the pending write escalated before the response deadline")
	}
	if err := world.clock.advance(t.Context(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := c.Step(t.Context(), bound, nil); err != nil {
		t.Fatal(err)
	}
	if c.IncidentOpen() || !closed.EscalatedAt.Equal(bound) || closed.HealthClearedAt != nil || len(closed.FinalDetectorStates) != 0 || closed.UnconfirmedDetail != "write never settled" {
		t.Fatalf("response deadline did not freeze an unverified unsettled-write escalation: open=%v escalated=%v detail=%q", c.IncidentOpen(), closed.EscalatedAt, closed.UnconfirmedDetail)
	}
	outcome, err := (ProtoOutcomeDeriver{}).Derive(t.Context(), *closed, "", "synthetic", "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	record := outcome.Record
	attempt := deterministicStageReport(closed.Request.PriorStages).GetMitigations()[0]
	trust := ProgramTrustStats([]*contracts.OutcomeRecord{record})[attempt.GetPlaybookId()]
	if record.GetClassification() != outcomeclass.Escalated || record.GetTimestamps().GetVerifiedAt() != nil || !attempt.GetRolloutUnsettledAtDeadline() || trust.Failures != 1 {
		t.Fatalf("escalation lost its rollout or trust evidence: classification=%s, attempt=%v, trust=%+v", record.GetClassification(), attempt, trust)
	}
}

func TestAutomaticStageDeadlineHandsOffWholeUnresolvedSet(t *testing.T) {
	extra := &deadlineConfirmationProbe{
		windowedSettlingDetector: windowedSettlingDetector{settlingDetector: settlingDetector{id: "traffic-other", class: sdk.DetectorClassHealth}},
		unavailable:              true,
	}
	c, world, _ := automaticRecoveryWorld(t, settlingCase{}, settings.AfterPlaybookAgent, extra)
	extra.clock = world.clock
	joined := sdk.Finding{DetectorID: extra.id, RuleID: "probe.other", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "other"}}
	c.currentIncidentRequest.Findings = append(c.currentIncidentRequest.Findings, joined)
	c.currentIncidentRequest.JoinedKeys = []string{findingKey(joined)}
	report := autoReport(c)
	autoStep(t, c, world.clock, report.GetRecoveryDeadline().AsTime().Sub(world.clock.Now()))
	want := slices.Clone(alertKeys(c.currentIncidentRequest))
	slices.Sort(want)
	if !report.GetHandoffSelected() || !slices.Equal(report.GetHandoffKeys(), want) || report.GetStabilityDeadline().IsValid() {
		t.Fatalf("handoff keys = %v, want %v", report.GetHandoffKeys(), want)
	}
}
