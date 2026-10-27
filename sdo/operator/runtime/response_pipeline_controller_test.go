package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

type pipelineScenarioDispatcher struct {
	loop       *ResponderLoop
	agentCalls int
}

func (d *pipelineScenarioDispatcher) DeterministicResponseStage() ResponseStage {
	return &DeterministicResponseStage{NewLoop: func(request IncidentRequest) *ResponderLoop { d.loop.Request = request; return d.loop }}
}
func (d *pipelineScenarioDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.agentCalls++
	return completedResult(request.IncidentID), nil
}

// Exercise admission, real Kubernetes-plan writes, confirmed health observations
// and ordinary controller closure. No worker is created for the two repairs.
func TestHermeticResponsePipelineClearsTwoFaultsThroughControllerClosure(t *testing.T) {
	loop, state := pipelineTestLoop(t, false)
	dispatcher := &pipelineScenarioDispatcher{loop: loop}
	config := testControllerConfig()
	config.Application = state.Request.Application
	config.Namespace = "app"
	config.SourceCommit = state.Request.SourceCommit
	config.DeployedCommit = state.Request.DeployedCommit
	config.RepositoryWorktree = t.TempDir()
	config.RepairPolicy = "recorded-actions"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Millisecond
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan IncidentClosure, 1)
	controller.OnIncidentClosed = func(closure IncidentClosure) { closed <- closure }
	for i := 0; i < 3; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("pipeline was not admitted")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	awaitDispatchCompletionQueued(t, controller)
	for i := 3; i < 23; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case closure := <-closed:
		if dispatcher.agentCalls != 0 || closure.Result == nil || closure.Result.ResolutionPath != "deterministic" || len(closure.Request.PriorStages) != 1 || len(closure.Result.RepairActions) != 2 || len(closure.Result.ConfirmedRootCauses) != 2 {
			t.Fatalf("pipeline closure lost evidence or ran agent: %+v agent=%d", closure, dispatcher.agentCalls)
		}
		if len(closure.FinalDetectorStates) == 0 || closure.FinalDetectorStates[0].Status != DetectorEvaluationClear {
			t.Fatalf("closure bypassed independent health: %+v", closure.FinalDetectorStates)
		}
		typed, err := incidentClosureContract(closure)
		if err != nil {
			t.Fatal(err)
		}
		derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
		if err != nil {
			t.Fatal(err)
		}
		if got := validate.VerifiedReflectionCauses(derived.Record); len(got) != 2 {
			t.Fatalf("real controller closure lost independently verified deterministic causes: %v", got)
		}
		if got := partialRepairCauses(derived.Record); len(got) != 2 {
			t.Fatalf("deterministic applications lost scoped reflection evidence: %v", got)
		}
		if derived.Record.Classification != "success" || len(derived.Record.PriorStages) != 1 || derived.Record.ResolutionPath != "deterministic" || len(typed.GetRequest().GetPlaybookCoverage().GetMatches()) != 2 {
			t.Fatalf("typed outcome lost pipeline evidence: %v", derived.Record)
		}
	case <-time.After(time.Second):
		t.Fatal("deterministic stage did not reach real controller closure")
	}
}

// recordingScenarioDispatcher hands each agent request to the test over a
// channel, because Dispatch runs on the controller's dispatch goroutine.
type recordingScenarioDispatcher struct {
	pipelineScenarioDispatcher
	requests chan IncidentRequest
}

func (d *recordingScenarioDispatcher) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.requests <- request
	return completedResult(request.IncidentID), nil
}

// With one of two faults covered, the controller applies the covered playbook
// and the agent is dispatched once, told the stage report and given only the
// remaining finding.
func TestHermeticResponsePipelineHandsAgentStageReportAndRemainingFindings(t *testing.T) {
	loop, state := pipelineTestLoop(t, true)
	dispatcher := &recordingScenarioDispatcher{pipelineScenarioDispatcher: pipelineScenarioDispatcher{loop: loop}, requests: make(chan IncidentRequest, 4)}
	config := testControllerConfig()
	config.Application = state.Request.Application
	config.Namespace = "app"
	config.SourceCommit = state.Request.SourceCommit
	config.DeployedCommit = state.Request.DeployedCommit
	config.RepositoryWorktree = t.TempDir()
	config.RepairPolicy = "recorded-actions"
	config.FiringThreshold = 1
	config.ClearThreshold = 1
	config.BatchDebounce = time.Millisecond
	start := time.Now().UTC()
	controller, err := NewController(config, []sdk.Detector{pipelineDetector{}}, loop.Provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := controller.Step(context.Background(), start.Add(time.Duration(i)*time.Second), nil); err != nil {
			t.Fatal(err)
		}
	}
	effect, ok := controller.PendingDispatchEffect()
	if !ok {
		t.Fatal("pipeline was not admitted")
	}
	if err := controller.ExecuteDispatchEffect(context.Background(), effect); err != nil {
		t.Fatal(err)
	}
	request := awaitRequest(t, dispatcher.requests)
	if len(request.PriorStages) != 1 || len(request.PriorStages[0].Mitigations) != 1 || request.PriorStages[0].Mitigations[0].Result != "applied_cleared" {
		t.Fatalf("agent was not told the stage result: %+v", request.PriorStages)
	}
	if len(request.Findings) != 1 {
		t.Fatalf("agent should receive only the uncovered finding, got %v", request.Findings)
	}
}
