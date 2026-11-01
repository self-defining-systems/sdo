package runtime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
)

// runDeterministicStage runs the automatic stage ahead of the agent. It returns
// a completed result when the stage's own independent observation found
// nothing open; otherwise the request handed to the agent carries what the
// stage did and only the findings that remain.
func (d KubernetesJobDispatcher) runDeterministicStage(ctx context.Context, request IncidentRequest, loop *ResponderLoop) (IncidentResult, IncidentRequest, bool, error) {
	started := loop.clock().UTC()
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...)}
	stage := &DeterministicStage{NewLoop: func(IncidentRequest) *ResponderLoop { return loop }}
	if err := RunResponseStages(ctx, state, []ResponseStage{stage}); err != nil {
		return IncidentResult{}, request, false, err
	}
	if len(state.Findings) == 0 && state.Healthy {
		result := deterministicResult(request, loop, state, started)
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		if err := validateIncidentResultFor(result, request); err != nil {
			return IncidentResult{}, request, false, err
		}
		return result, request, true, nil
	}
	request.PriorStages = cloneStageReports(state.PriorStages)
	if len(state.PriorStages) > 0 && len(state.PriorStages[0].GetMitigations()) > 0 {
		request.Findings = append([]sdk.Finding(nil), state.Findings...)
	}
	return IncidentResult{}, request, false, nil
}

// handoffTask rewrites the frozen agent task so the agent sees the stage's
// report and the findings that remain.
func (d KubernetesJobDispatcher) handoffTask(request IncidentRequest, task *contracts.AgentTask) (*contracts.AgentTask, IncidentRequest, error) {
	prepared := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(prepared); err != nil {
		return nil, request, fmt.Errorf("decode prepared incident for stage handoff: %w", err)
	}
	projected, err := incidentRequestContract(request)
	if err != nil {
		return nil, request, err
	}
	prepared.PriorStages = projected.PriorStages
	prepared.Findings = projected.Findings
	prepared.FastPathSummary = request.FastPathSummary
	task.Request, err = anypb.New(prepared)
	if err != nil {
		return nil, request, err
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return nil, request, err
	}
	request.PreparedAgentTask = string(payload)
	rewritten := new(contracts.AgentTask)
	if err := protojson.Unmarshal(payload, rewritten); err != nil {
		return nil, request, fmt.Errorf("decode stage handoff task: %w", err)
	}
	return rewritten, request, nil
}

// deterministicResult is the incident result when the stage cleared every
// finding. The evidence is the stage's own plan applications and diagnoses.
func deterministicResult(request IncidentRequest, loop *ResponderLoop, state *IncidentState, started time.Time) IncidentResult {
	completed := loop.clock().UTC()
	result := IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted,
		Timing:              TimingMetrics{StartedAt: started, CompletedAt: completed},
		FinalDetectorStates: append([]DetectorEvaluation(nil), state.DetectorStates...),
		ProgramToolCalls:    loop.ToolCalls(),
		ProgramRuns:         loop.ProgramRuns(), PlanApplications: loop.PlanApplications(), ProgramFailureSignals: loop.FailureSignals(),
		ResponseLoopsUsed: loop.LoopsUsed(), ResolutionPath: "deterministic", PriorStages: cloneStageReports(state.PriorStages),
		VerificationEvidence: []VerificationEvidence{{Name: "saved program diagnosis and health", Passed: true, Details: "deterministic stage cleared every finding", ObservedAt: completed}},
	}
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, nil)
	for _, finding := range request.Findings {
		result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, ConfirmedRootCause{
			Summary: finding.Summary, Resources: []sdk.ObjectRef{finding.PrimaryResource},
			Evidence:           []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}},
			ExplainedDetectors: []string{finding.DetectorID},
		})
	}
	for _, report := range state.PriorStages {
		for _, attempt := range report.GetMitigations() {
			if attempt.GetResult() == "applied_cleared" {
				result.AppliedPlaybooks = append(result.AppliedPlaybooks, AppliedPlaybook{PlaybookID: attempt.GetPlaybookId()})
			}
		}
	}
	return result
}
