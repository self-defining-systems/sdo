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
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// The real dispatcher must return a terminal worker result to its controller
// even when a detector never finishes its observation window.
func TestUnconfirmedResponseContinuesOnceThenReturnsToClosureOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	client := fake.NewSimpleClientset()
	now := time.Now().UTC()
	snapshot := sdktest.Snapshot{}
	request := goldenRequest(t)
	request.RepositoryWorktree = t.TempDir() + "/proposal"
	request.ResponseDeadline = now.Add(time.Minute)
	request.MaxResponseLoops = 4
	controller, err := NewController(testControllerConfig(), []sdk.Detector{&windowDetector{}}, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, now)
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
	loop := NewResponderLoop(request, nil, staticProvider{snapshot: snapshot}, nil, PlanExecutor{}, 4)
	loop.VerifyResult = func(context.Context, IncidentResult) ToolResponse {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		_, healthy := controller.finalVerificationStates()
		return controller.continuationVerificationLocked(healthy, closureSourceReport{})
	}
	d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(request.RepositoryWorktree), ServiceAccount: "responder", PollInterval: time.Millisecond, LoopFactory: func(IncidentRequest) *ResponderLoop { return loop }}
	controller.dispatcher = d
	broker := &recordingClosureBroker{}
	if err := controller.SetIncidentBroker(broker); err != nil {
		t.Fatal(err)
	}
	done := make(chan dispatchCompletion, 1)
	dispatch := func() {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := d.Dispatch(ctx, request)
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
	publishContinuationResult(t, client, first, completedResult(request.IncidentID))
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
	publishContinuationResult(t, client, second, completedResult(request.IncidentID))
	finish()
	// Restore the durable dispatcher state into a fresh loop: restart cannot
	// authorize another blocker continuation.
	restored := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 4)
	restarted := d
	restarted.LoopFactory = func(IncidentRequest) *ResponderLoop { return restored }
	verification := loop.VerifyResult(ctx, completedResult(request.IncidentID))
	resumed, err := restarted.ContinueAfterVerification(ctx, request, verification, now)
	if err != nil || resumed {
		t.Fatalf("restart repeated blocker continuation: %v %v", resumed, err)
	}
	controller.maybeCloseIncident(ctx, now, snapshot)
	if !controller.responderDone || controller.pendingClosure != nil {
		t.Fatal("unconfirmed verification did not wait within its bound")
	}
	now = request.ResponseDeadline
	controller.maybeCloseIncident(ctx, now, snapshot)
	if controller.pendingClosure == nil || controller.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
		t.Fatalf("missing unconfirmed closure at bound: %+v", controller.pendingClosure)
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
}

func TestUnconfirmedVerificationClosesWithReceiptAtBound(t *testing.T) {
	for _, status := range []DetectorEvaluationStatus{DetectorEvaluationPending, DetectorEvaluationError} {
		t.Run(string(status), func(t *testing.T) {
			now := time.Unix(1000, 0).UTC()
			cfg := testControllerConfig()
			cfg.VerificationTimeout = 30 * time.Second
			snapshot := sdktest.Snapshot{}
			c, err := NewController(cfg, nil, staticProvider{snapshot: snapshot}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			broker := &recordingClosureBroker{}
			if err := c.SetIncidentBroker(broker); err != nil {
				t.Fatal(err)
			}
			c.now = func() time.Time { return now }
			request := goldenRequest(t)
			request.ResponseDeadline = now.Add(time.Hour)
			c.currentIncidentRequest = &request
			result := completedResult(request.IncidentID)
			c.currentIncidentResult = &result
			c.incidentOpen, c.responderDone = true, true
			c.responderCompletedAt = now
			c.healthDetectorIDs = []string{"health"}
			c.history = []DetectorEvaluation{{DetectorID: "health", Status: status, Reason: "window incomplete", Error: "observer unavailable", EvaluatedAt: now}}
			c.maybeCloseIncident(context.Background(), now, snapshot)
			if c.pendingClosure != nil {
				t.Fatal("closed before verification bound")
			}
			now = now.Add(cfg.VerificationTimeout)
			c.maybeCloseIncident(context.Background(), now, snapshot)
			if c.pendingClosure == nil || c.pendingClosure.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED || !strings.Contains(c.pendingClosure.UnconfirmedDetail, "health") {
				t.Fatalf("missing typed unconfirmed closure: %+v", c.pendingClosure)
			}
			executeRecheck(t, c)
			awaitBrokerResult(t, c.closureResults)
			c.processBrokerCompletions()
			if c.closureReceipt == nil || c.closureReceipt.OutcomeCommit == "" || c.closureFailure != nil {
				t.Fatalf("closure has no receipt or retired OwnerLost: receipt=%+v failure=%+v", c.closureReceipt, c.closureFailure)
			}
		})
	}
}
