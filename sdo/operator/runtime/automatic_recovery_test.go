package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/sdk"
)

// Drive the real stage and closure over the same synthetic clock and live
// rollout snapshots. The controller observes only when the test steps it.
func automaticRecoveryWorld(t *testing.T, tc settlingCase, policy string) (*Controller, *settlingWorld, *IncidentClosure) {
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
	controller, err := NewController(cfg, loop.Detectors[:1], world, &recordingDispatcher{}, now)
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
	if !restarted.escalatedAt.Equal(bound) || autoReport(restarted).GetHandoffSelected() || world.restarts.Load() != 1 {
		t.Fatal("the configured wait did not expire without an agent or replay")
	}
}

func TestAutomaticRecoveryOnlyHandsOffFreshFiringAlerts(t *testing.T) {
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
			if report.GetHandoffSelected() != tc.handoff || c.escalatedAt.IsZero() == !tc.handoff {
				t.Fatalf("handoff = %v, escalated at %s, want handoff = %v", report.GetHandoffSelected(), c.escalatedAt, tc.handoff)
			}
			if !tc.failing && !report.GetMitigations()[0].GetRolloutUnsettledAtDeadline() {
				t.Fatal("unsettled rollout at expiry did not record terminal evidence")
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
