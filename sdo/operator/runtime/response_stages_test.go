package runtime

import (
	"context"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
)

type clearingResponseStage struct {
	name  string
	calls *int
}

func (s clearingResponseStage) Name() string { return s.name }
func (s clearingResponseStage) Run(_ context.Context, state *IncidentState) (*contracts.StageReport, error) {
	*s.calls++
	state.Findings = nil
	return &contracts.StageReport{Stage: s.name}, nil
}
func TestResponsePipelineStopsWhenFindingsClear(t *testing.T) {
	first, second := 0, 0
	state := &IncidentState{Findings: []sdk.Finding{{DetectorID: "fault"}}}
	err := RunResponseStages(context.Background(), state, []ResponseStage{clearingResponseStage{"first", &first}, clearingResponseStage{"second", &second}})
	if err != nil || first != 1 || second != 0 || len(state.PriorStages) != 1 {
		t.Fatalf("pipeline: %+v, %v, calls %d/%d", state, err, first, second)
	}
}

func TestResponseStageConfigSelectsBaselineAndRejectsAmbiguity(t *testing.T) {
	for _, names := range [][]string{{"agent"}, {"deterministic", "agent"}, {"deterministic"}} {
		if _, err := responseStageNames(names); err != nil {
			t.Fatal(err)
		}
	}
	for _, names := range [][]string{{""}, {"agent", "agent"}, {"trust-gate"}} {
		if _, err := responseStageNames(names); err == nil {
			t.Fatalf("accepted stages %v", names)
		}
	}
	defaults, err := responseStageNames(nil)
	if err != nil || len(defaults) != 2 || defaults[0] != "deterministic" || defaults[1] != "agent" {
		t.Fatalf("default stages %v: %v", defaults, err)
	}
}

func TestResponseStageAgentOnlyPreservesExactDispatchContract(t *testing.T) {
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller := &Controller{codePath: &CodePath{Selection: CodePathSelection{ResponseStages: []string{"agent"}}}, dispatcher: dispatcher}
	request := goldenRequest(t)
	request.PreparedAgentTask = "frozen-original-task"
	expected := completedResult(request.IncidentID)
	result, reports, err := controller.runResponsePipeline(context.Background(), request)
	got := <-dispatcher.requests
	if err != nil || len(reports) != 0 || !sameIncidentRequest(got, request) || result.Status != expected.Status || result.IncidentID != expected.IncidentID {
		t.Fatalf("baseline changed: request=%+v, result=%+v, reports=%v, err=%v", got, result, reports, err)
	}
}

func TestResponseStageMergesOverlappingFindingCausesWithoutLosingResources(t *testing.T) {
	a := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "a"}
	b := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "b"}
	c := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "c"}
	causes := []ConfirmedRootCause{{Summary: "first", Resources: []sdk.ObjectRef{a, b}, ExplainedDetectors: []string{"one"}}}
	causes = mergeStageCause(causes, ConfirmedRootCause{Summary: "second", Resources: []sdk.ObjectRef{b, c}, ExplainedDetectors: []string{"two"}})
	if len(causes) != 1 || len(causes[0].Resources) != 3 || len(causes[0].ExplainedDetectors) != 2 {
		t.Fatalf("lost or multiply owned stage cause: %+v", causes)
	}
}

type rejoiningStageDispatcher struct{ automatic, agent int }

func (d *rejoiningStageDispatcher) DeterministicResponseStage() ResponseStage {
	return clearingResponseStage{name: "deterministic", calls: &d.automatic}
}
func (d *rejoiningStageDispatcher) Dispatch(_ context.Context, request IncidentRequest) (IncidentResult, error) {
	d.agent++
	result := completedResult(request.IncidentID)
	result.ResponderSessionID = "resumed-session"
	return result, nil
}
func TestResponseStageRejoinKeepsAgentOwnershipAndReflectionEvidence(t *testing.T) {
	dispatcher := &rejoiningStageDispatcher{}
	controller := &Controller{codePath: &CodePath{Selection: CodePathSelection{ResponseStages: []string{"deterministic", "agent"}}}, dispatcher: dispatcher, responseStagesRunning: true}
	request := goldenRequest(t)
	request.PreparedAgentTask = "durable-agent-handoff"
	request.PriorStages = []*contracts.StageReport{{Stage: "deterministic"}}
	result, reports, err := controller.runResponsePipeline(context.Background(), request)
	if err != nil || dispatcher.automatic != 0 || dispatcher.agent != 1 || result.ResponderSessionID != "resumed-session" || result.ResolutionPath == "deterministic" || len(reports) != 1 || controller.responseStagesRunning {
		t.Fatalf("agent handoff lost ownership/evidence: automatic=%d agent=%d result=%+v reports=%v running=%t err=%v", dispatcher.automatic, dispatcher.agent, result, reports, controller.responseStagesRunning, err)
	}
}
