package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

// delayedPortDetector reports the wrong-port fault only once released, like a
// learned detector that activates after the responder was dispatched.
type delayedPortDetector struct {
	pipelineDetector
	released *atomic.Bool
}

func (d delayedPortDetector) Detect(ctx context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings, err := d.pipelineDetector.Detect(ctx, view)
	if d.released.Load() {
		return findings, err
	}
	kept := findings[:0]
	for _, finding := range findings {
		if finding.RuleID != "wrong-port" {
			kept = append(kept, finding)
		}
	}
	return kept, err
}

// blockingAgent holds the agent stage open, as a responder at work does.
type blockingAgent struct {
	pipelineScenarioDispatcher
	requests chan IncidentRequest
	release  chan struct{}
}

func (d *blockingAgent) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	<-d.release
	return completedResult(request.IncidentID), nil
}

// The real controller activates a finding mid-incident, the real stage applies
// the learned playbook on the responder's loop, and the real outcome path and
// the responder's tool reply carry the result.
func TestLearnedMatchThatActivatesMidIncidentIsAppliedWithoutWaitingForTheAgent(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	released := &atomic.Bool{}
	detector := delayedPortDetector{released: released}
	dispatcher := &blockingAgent{pipelineScenarioDispatcher: pipelineScenarioDispatcher{loop: loop}, requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	config := testControllerConfig()
	config.Application, config.Namespace = state.Request.Application, "app"
	config.SourceCommit, config.DeployedCommit = state.Request.SourceCommit, state.Request.DeployedCommit
	config.RepositoryWorktree = t.TempDir()
	config.RepairPolicy = "recorded-actions"
	config.FiringThreshold, config.ClearThreshold = 1, 1
	config.BatchDebounce = time.Millisecond
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{detector}, loop.Provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	step := func(i int) {
		t.Helper()
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		step(i)
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("incident was not admitted")
	}
	loop.Request = effect.Request
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	opening := awaitRequest(t, dispatcher.requests)
	if len(opening.Findings) != 1 || len(loop.PlanApplications()) != 0 {
		t.Fatalf("opening: findings=%v applications=%d", opening.Findings, len(loop.PlanApplications()))
	}
	// The responder is working on its loop; the learned detector now fires.
	controller.mu.Lock()
	controller.responderLoop = loop
	controller.mu.Unlock()
	released.Store(true)
	step(3)
	deadline := time.Now().Add(5 * time.Second)
	for len(loop.PlanApplications()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(loop.PlanApplications()) != 1 {
		t.Fatalf("the late learned match was not applied while the agent worked: %v", loop.PlanApplications())
	}
	for time.Now().Before(deadline) {
		controller.mu.Lock()
		running := controller.midStage.running
		controller.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	controller.mu.Lock()
	stages := cloneStageReports(controller.currentIncidentRequest.PriorStages)
	findings := len(controller.currentIncidentRequest.Findings)
	controller.mu.Unlock()
	if len(stages) != 1 || len(stages[0].Mitigations) != 1 || stages[0].Mitigations[0].PlaybookId != "service-targetport-mismatch" || stages[0].Mitigations[0].Result != "applied_cleared" || findings != 2 {
		t.Fatalf("incident did not record the late pass: stages=%v findings=%d", stages, findings)
	}
	// The responder's next reply says so, once.
	reply := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(reply.PriorStages) != 1 || reply.PriorStages[0].PlaybookID != "service-targetport-mismatch" || reply.PriorStages[0].Result != "applied_cleared" {
		t.Fatalf("responder was not told: %+v", reply.PriorStages)
	}
	if again := loop.Handle(context.Background(), ToolRequest{Command: "status"}); len(again.PriorStages) != 0 {
		t.Fatalf("note repeated: %+v", again.PriorStages)
	}
	// The agent's completion does not erase the late pass.
	close(dispatcher.release)
	awaitDispatchCompletionQueued(t, controller)
	step(4)
	// The incident result, which learning attribution and analysis read, names
	// the playbook the late pass applied, not only the opening stage's.
	controller.mu.Lock()
	final := cloneIncidentResult(controller.currentIncidentResult)
	controller.mu.Unlock()
	if final == nil {
		t.Fatal("no incident result after the agent completed")
	}
	applied := false
	for _, playbook := range final.AppliedPlaybooks {
		applied = applied || playbook.PlaybookID == "service-targetport-mismatch"
	}
	if !applied || len(final.ConfirmedRootCauses) == 0 {
		t.Fatalf("result omits the late pass: applied=%+v causes=%+v", final.AppliedPlaybooks, final.ConfirmedRootCauses)
	}
	controller.mu.Lock()
	kept := cloneStageReports(controller.currentIncidentRequest.PriorStages)
	controller.mu.Unlock()
	if len(kept) < 1 || len(kept[0].Mitigations) != 1 || kept[0].Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("agent completion erased the late pass: %v", kept)
	}
}

// Every pass's notes reach the agent: a stage's own plan and apply calls run
// through Handle but never take what an earlier pass queued for the agent.
func TestNotesFromEveryPassSurviveTheStagesOwnCallsUntilTheAgentCalls(t *testing.T) {
	loop, _ := pipelineTestLoop(t, true)
	loop.noteStages([]StageNote{{Stage: "deterministic", PlaybookID: "first-pass-playbook", Result: "applied_cleared"}})
	stageCall := loop.Handle(context.Background(), ToolRequest{Command: "plan", ProgramID: "service-targetport-mismatch", stage: true})
	if len(stageCall.PriorStages) != 0 {
		t.Fatalf("the stage's own call took the agent's notes: %+v", stageCall.PriorStages)
	}
	loop.noteStages([]StageNote{{Stage: "deterministic", PlaybookID: "second-pass-playbook", Result: "applied_cleared"}})
	loop.Handle(context.Background(), ToolRequest{Command: "apply", PlanIDs: []string{"missing"}, stage: true})
	reply := loop.Handle(context.Background(), ToolRequest{Command: "status"})
	if len(reply.PriorStages) != 2 || reply.PriorStages[0].PlaybookID != "first-pass-playbook" || reply.PriorStages[1].PlaybookID != "second-pass-playbook" {
		t.Fatalf("the agent was not told about both passes: %+v", reply.PriorStages)
	}
	if again := loop.Handle(context.Background(), ToolRequest{Command: "status"}); len(again.PriorStages) != 0 {
		t.Fatalf("notes repeated: %+v", again.PriorStages)
	}
}
