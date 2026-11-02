//go:build integration

package runtime

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The real dispatcher must return a terminal worker result to its controller
// even when a detector never finishes its observation window.
func TestUnconfirmedResponseContinuesOnceThenReturnsToClosureOwner(t *testing.T) {
	for _, mode := range []string{"completed", "running", "running-restart", "running-restart-unavailable", "running-restart-paused", "running-stop-retry"} {
		t.Run(mode, func(t *testing.T) { testUnconfirmedResponseContinuation(t, mode) })
	}
}

func testUnconfirmedResponseContinuation(t *testing.T, mode string) {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	client := fake.NewSimpleClientset()
	now := time.Now().UTC()
	snapshot := sdktest.Snapshot{}
	request := goldenRequest(t)
	repository, err := broker.New(nativeRepository(t), filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	reflections := 0
	native := &NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test",
		Reflector: ReflectionWorkerFunc(func(_ context.Context, _ IncidentClosure, _ IncidentWorkspace, outcome *contracts.OutcomeRecord, _, _ string, _ ReflectionFeedback) (ReflectionDecision, error) {
			reflections++
			if outcome.GetClassification() != "unconfirmed" {
				t.Errorf("reflection received %q", outcome.GetClassification())
			}
			return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "observation blockers remain unconfirmed"}, nil
		}),
	}
	workspace, err := native.PrepareIncident(ctx, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	request.RepositoryWorktree, request.RepositoryBaseCommit = workspace.Worktree, workspace.BaseCommit
	request.SourceCommit, request.DeployedCommit = workspace.BaseCommit, workspace.BaseCommit

	request.ResponseDeadline = now.Add(time.Minute)
	request.MaxResponseLoops = 2
	cfg := testControllerConfig()
	cfg.VerificationTimeout = 30 * time.Second
	controller, err := NewController(cfg, []sdk.Detector{&windowDetector{}}, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, now)
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return now }
	// Run the real detector producer; its pending evaluation feeds the real
	// controller verification, dispatcher, Python renderer and outcome consumer.
	if err := controller.Step(ctx, now, nil); err != nil {
		t.Fatal(err)
	}
	controller.currentIncidentRequest = &request
	controller.incidentOpen = true
	controller.incidentDetectedAt, controller.incidentDispatchedAt = now, now
	controller.healthDetectorIDs = []string{"health"}
	loop := NewResponderLoop(request, nil, staticProvider{snapshot: snapshot}, nil, PlanExecutor{}, 2)
	loop.VerifyResult = func(ctx context.Context, result IncidentResult) ToolResponse {
		return controller.responderResultVerification(ctx, request.IncidentID, &result)
	}
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	controller.dispatcher = d
	if err := controller.SetIncidentBroker(native); err != nil {
		t.Fatal(err)
	}
	done := make(chan dispatchCompletion, 1)
	dispatch := func() {
		dispatchCtx, stop := context.WithCancel(ctx)
		controller.responderCancel = stop
		controller.dispatchState = contracts.DispatchState_DISPATCH_STATE_RUNNING
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer stop()
			result, err := d.Dispatch(dispatchCtx, request)
			done <- dispatchCompletion{incidentID: request.IncidentID, result: result, err: err}
		}()
	}
	finish := func() {
		t.Helper()
		select {
		case completion := <-done:
			if completion.err != nil {
				t.Fatal(completion.err)
			}
			controller.handleDispatchCompletion(completion, now)
		case <-time.After(3 * time.Second):
			t.Fatal("dispatcher held closure owner hostage to an unconfirmed detector")
		}
	}
	dispatch()
	first := awaitJob(t, client, IncidentJobName(request.IncidentID))
	workerResult := completedResult(request.IncidentID)
	workerResult.ResponderSessionID = "scripted-unconfirmed-session"
	publishContinuationResult(t, client, first, workerResult)
	finish()
	controller.maybeCloseIncident(ctx, now, snapshot)
	if controller.responderDone {
		t.Fatal("named verification blockers did not resume the responder")
	}
	dispatch()
	second := awaitJob(t, client, IncidentJobName(request.IncidentID+"-continue-1"))
	feedback := ""
	for _, v := range second.Spec.Template.Spec.Containers[0].Env {
		if v.Name == "SDO_RESPONSE_CONTINUATION" {
			feedback = v.Value
		}
	}
	renderCtx, stopRender := context.WithTimeout(ctx, 8*time.Second)
	defer stopRender()
	command := exec.CommandContext(renderCtx, "uv", "run", "python", "-c", `import sys
from sdo.agents.responder.continuation import continuation_prompt
prompt=continuation_prompt(sys.stdin.read())
assert "health" in prompt and "observation window is incomplete" in prompt, prompt
assert len(prompt)<=1500, len(prompt)
`)
	command.Dir = filepath.Join("..", "..", "..")
	command.Stdin = strings.NewReader(feedback)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("real continuation consumer rejected feedback: %v %s", err, output)
	}
	if mode == "completed" {
		publishContinuationResult(t, client, second, workerResult)
		finish()
	} else if strings.Contains(mode, "restart") {
		state := exportState(t, controller)
		controller, err = NewController(cfg, []sdk.Detector{&windowDetector{}}, staticProvider{snapshot: snapshot}, d, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := controller.RestoreState(state); err != nil {
			t.Fatal(err)
		}
		controller.now = func() time.Time { return now }
		controller.healthDetectorIDs = []string{"health"}
		if err := controller.SetIncidentBroker(native); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(mode, "unavailable") {
			controller.provider = failingSnapshotProvider{}
		}
	}
	// Restore the durable dispatcher state into a fresh loop: restart cannot
	// authorize another blocker continuation.
	restored := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 2)
	restarted := d
	restarted.LoopFactory = func(IncidentRequest) *ResponderLoop { return restored }
	verification := loop.VerifyResult(ctx, completedResult(request.IncidentID))
	resumed, err := restarted.ContinueAfterVerification(ctx, request, verification, now)
	if err != nil || resumed != (mode != "completed") {
		t.Fatalf("restart did not preserve continuation state: %v %v", resumed, err)
	}
	controller.maybeCloseIncident(ctx, now, snapshot)
	if (mode == "completed" && !controller.responderDone) || controller.pendingClosure != nil {
		t.Fatal("unconfirmed verification did not wait within its bound")
	}
	deadlines := controller.scheduler.Deadlines()
	for id := range deadlines {
		deadlines[id] = now.Add(time.Hour)
	}
	if err := controller.scheduler.RestoreDeadlines(deadlines); err != nil {
		t.Fatal(err)
	}
	if wake := controller.NextWake(); wake.IsZero() || wake.After(now.Add(cfg.VerificationTimeout)) {
		t.Fatalf("running continuation lost verification wake: %v", wake)
	}
	now = now.Add(cfg.VerificationTimeout)
	if strings.Contains(mode, "stop-retry") {
		controller.dispatcher = &reportTailDispatcher{Dispatcher: d, stopError: errResponderTerminating}
		controller.maybeCloseIncident(ctx, now, snapshot)
		if controller.pendingClosure != nil {
			t.Fatal("closed before responder quiescence")
		}
		if wake := controller.NextWake(); !wake.Equal(now.Add(time.Second)) {
			t.Fatalf("termination retry wake %v", wake)
		}
		if wake := controller.PausedWake(); !wake.Equal(now.Add(time.Second)) {
			t.Fatalf("paused termination retry wake %v", wake)
		}
		select {
		case completion := <-done:
			controller.handleDispatchCompletion(completion, now)
		case <-time.After(10 * time.Second):
			t.Fatal("bounded continuation was not cancelled")
		}
		now = now.Add(time.Second)
		controller.dispatcher = d
	}
	if strings.Contains(mode, "paused") {
		if err := controller.stepPaused(ctx, now); err != nil {
			t.Fatal(err)
		}
	} else if strings.Contains(mode, "unavailable") {
		controller.maybeCloseIncident(ctx, now, nil)
	} else {
		controller.maybeCloseIncident(ctx, now, snapshot)
	}
	if controller.pendingClosure == nil || controller.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatalf("missing unconfirmed closure at bound: %+v", controller.pendingClosure)
	}
	if mode != "completed" {
		work, err := d.IncidentWork(ctx, request.IncidentID)
		if err != nil || work.Active {
			t.Fatalf("closure left responder running: %+v %v", work, err)
		}
		creates := 0
		for _, action := range client.Actions() {
			if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
				creates++
			}
		}
		if creates != 2 {
			t.Fatalf("wanted exactly one continuation, got %d Jobs", creates)
		}
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(ctx, *controller.pendingClosure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetClassification() != "unconfirmed" {
		t.Fatalf("wrong classification: %s", derived.Record.GetClassification())
	}
	named := false
	for _, state := range derived.Record.GetDetectorHistory() {
		if state.GetDetectorId() == "health" && strings.Contains(state.GetReason(), "observation window is incomplete") {
			named = true
		}
	}
	if !named {
		t.Fatalf("outcome lost detector blockers: %v", derived.Record)
	}
	executeRecheck(t, controller)
	awaitBrokerResult(t, controller.closureResults)
	controller.processBrokerCompletions()
	if controller.closureReceipt == nil || controller.closureReceipt.OutcomeCommit == "" || controller.closureFailure != nil {
		t.Fatalf("unconfirmed closure missing receipt: %+v %+v", controller.closureReceipt, controller.closureFailure)
	}
	if _, err := closeAndReflect(t, ctx, native, *controller.pendingClosure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if reflections != 1 || !ledger.GetReflectionProcessed() || ledger.GetOutcomeDocument().GetClassification() != "unconfirmed" {
		t.Fatalf("unconfirmed outcome did not reach reflection: calls=%d ledger=%v", reflections, ledger)
	}
}

