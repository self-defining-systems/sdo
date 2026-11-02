//go:build integration

package runtime

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

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
	controller.mu.Lock()
	coverage := cloneCoverage(controller.currentIncidentRequest.PlaybookCoverage)
	runs := append([]ProgramRun(nil), controller.currentIncidentRequest.InitialProgramRuns...)
	controller.mu.Unlock()
	if len(coverage.GetMatches()) == 0 || len(coverage.GetMatches()[0].GetExplainedFindingFingerprints()) == 0 || !slices.ContainsFunc(runs, func(run ProgramRun) bool { return run.Verdict.Result == program.Match }) {
		t.Fatalf("late stage receipts lost their coverage/diagnosis evidence: coverage=%v runs=%v", coverage, runs)
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
	controller.mu.Lock()
	finalCoverage := cloneCoverage(controller.currentIncidentRequest.PlaybookCoverage)
	controller.mu.Unlock()
	if len(finalCoverage.GetMatches()) == 0 || len(finalCoverage.GetMatches()[0].GetExplainedFindingFingerprints()) == 0 {
		t.Fatalf("pipeline completion erased late coverage: %v", finalCoverage)
	}
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

// A later pass that clears nothing and applies no program new to the incident
// settles the stage. The next activation joins the incident for the responder
// without another pass.
func TestNoProgressLaterPassSettlesTheStage(t *testing.T) {
	loop, state := pipelineTestLoopWithServices(t, true, "api2", "api3")
	registry, err := program.NewRegistry([]program.Diagnoser{pipelineIneffectivePortProgram{}}, []program.Mitigator{pipelineIneffectivePortProgram{}})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	released := &atomic.Int32{}
	detector := stagedPortDetector{order: []string{"api", "api2", "api3"}, released: released}
	dispatcher := &blockingAgent{pipelineScenarioDispatcher: pipelineScenarioDispatcher{loop: loop}, requests: make(chan IncidentRequest, 1), release: make(chan struct{})}
	defer close(dispatcher.release)
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
	awaitRequest(t, dispatcher.requests)
	controller.mu.Lock()
	controller.responderLoop = loop
	controller.mu.Unlock()
	idle := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			controller.mu.Lock()
			running := controller.midStage.running
			controller.mu.Unlock()
			if !running {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatal("later pass did not finish")
	}
	// api: the program applies, clears nothing and is reverted, so the pass
	// left nothing behind (no progress) and settles the stage; api2 then joins
	// without another write.
	for i, want := range []int{1, 1} {
		released.Store(int32(i + 1))
		step(3 + i)
		idle()
		if got := len(loop.PlanApplications()); got != want {
			t.Fatalf("pass %d: %d applications, want %d", i+1, got, want)
		}
	}
	released.Store(3)
	step(5)
	idle()
	controller.mu.Lock()
	passes := controller.midStage.passes
	joined := false
	for _, finding := range controller.currentIncidentRequest.Findings {
		joined = joined || finding.PrimaryResource.Name == "api3"
	}
	controller.mu.Unlock()
	if passes != 1 || len(loop.PlanApplications()) != 1 || !loop.PlanApplications()[0].RolledBack {
		t.Fatalf("a settled stage ran again: passes=%d applications=%d", passes, len(loop.PlanApplications()))
	}
	if !joined {
		t.Fatal("the later finding did not join the incident for the responder")
	}
}
