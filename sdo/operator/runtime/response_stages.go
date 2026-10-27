package runtime

import (
	"context"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// ResponseStage operates on current evidence and reports its own actions. It
// neither chooses a later stage nor grants authority to close an incident.
type ResponseStage interface {
	Name() string
	Run(context.Context, *IncidentState) (*contracts.StageReport, error)
}

// IncidentState is local to one ordered response attempt. Request retains the
// opening evidence; Findings and UnexplainedFindings describe remaining work.
type IncidentState struct {
	Request             IncidentRequest
	Findings            []sdk.Finding
	Coverage            *contracts.PlaybookCoverage
	PriorStages         []*contracts.StageReport
	UnexplainedFindings []sdk.Finding
	Result              *IncidentResult
	RepairActions       []RepairActionReceipt
	ProgramToolCalls    []ToolCall
	// Refresh marks a later pass over an incident whose opening pass already
	// ran: Findings are re-observed and coverage is rebuilt, and attempts the
	// stage recorded earlier are never repeated.
	Refresh bool
}

// RunResponseStages stops when no findings remain. Every attempted stage has a
// report, including errors, so subsequent workers never infer success by silence.
func RunResponseStages(ctx context.Context, state *IncidentState, stages []ResponseStage) error {
	state.refreshUnexplained()
	for _, stage := range stages {
		if len(state.Findings) == 0 {
			break
		}
		before := findingFingerprints(state.Findings)
		report, err := stage.Run(ctx, state)
		if report == nil {
			report = &contracts.StageReport{}
		}
		state.refreshUnexplained()
		report.Stage = stage.Name()
		remaining := map[string]bool{}
		for _, fp := range findingFingerprints(state.Findings) {
			remaining[fp] = true
		}
		report.ClearedFindingFingerprints = nil
		for _, fp := range before {
			if !remaining[fp] {
				report.ClearedFindingFingerprints = append(report.ClearedFindingFingerprints, fp)
			}
		}
		report.RemainingFindingFingerprints = nil
		for _, finding := range state.Findings {
			report.RemainingFindingFingerprints = append(report.RemainingFindingFingerprints, FindingFingerprint(finding))
		}
		if err != nil {
			report.Error = err.Error()
		}
		state.PriorStages = append(state.PriorStages, report)
		if err != nil {
			return fmt.Errorf("response stage %s: %w", stage.Name(), err)
		}
	}
	return nil
}

// RunStageAgain runs a stage a second (or later) time over an incident whose
// earlier pass is already in state.PriorStages. The stage's one report is
// updated in place of the earlier one: its attempts keep their order and
// receipts, new attempts follow, and clearance accumulates.
func RunStageAgain(ctx context.Context, state *IncidentState, stage ResponseStage) (*contracts.StageReport, error) {
	var earlier *contracts.StageReport
	index := -1
	for i, prior := range state.PriorStages {
		if prior.GetStage() == stage.Name() {
			earlier, index = prior, i
		}
	}
	state.Refresh = true
	defer func() { state.Refresh = false }()
	report, err := stage.Run(ctx, state)
	if report == nil {
		report = &contracts.StageReport{}
	}
	state.refreshUnexplained()
	report.Stage = stage.Name()
	remaining := map[string]bool{}
	report.RemainingFindingFingerprints = nil
	for _, finding := range state.Findings {
		fp := FindingFingerprint(finding)
		remaining[fp] = true
		report.RemainingFindingFingerprints = append(report.RemainingFindingFingerprints, fp)
	}
	cleared := map[string]bool{}
	for _, fp := range earlier.GetClearedFindingFingerprints() {
		cleared[fp] = true
	}
	for _, fp := range findingFingerprints(state.Request.Findings) {
		if !remaining[fp] {
			cleared[fp] = true
		}
	}
	report.ClearedFindingFingerprints = nil
	for fp := range cleared {
		report.ClearedFindingFingerprints = append(report.ClearedFindingFingerprints, fp)
	}
	sort.Strings(report.ClearedFindingFingerprints)
	if err != nil {
		report.Error = err.Error()
	}
	if index >= 0 {
		state.PriorStages[index] = report
	} else {
		state.PriorStages = append(state.PriorStages, report)
	}
	if err != nil {
		return report, fmt.Errorf("response stage %s: %w", stage.Name(), err)
	}
	return report, nil
}

func cloneStageReports(reports []*contracts.StageReport) []*contracts.StageReport {
	var result []*contracts.StageReport
	for _, report := range reports {
		result = append(result, proto.Clone(report).(*contracts.StageReport))
	}
	return result
}

// AgentResponseStage adapts the current responder without changing its tool or
// continuation semantics. Earlier actions arrive before its first model turn.
type AgentResponseStage struct {
	Dispatcher Dispatcher
	Prepare    func(context.Context, IncidentRequest) (IncidentRequest, error)
}

func (AgentResponseStage) Name() string { return "agent" }
func (stage AgentResponseStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	request := *cloneIncidentRequest(&state.Request)
	if len(state.PriorStages) > 0 {
		request.PriorStages = cloneStageReports(state.PriorStages)
		request.Findings = append([]sdk.Finding(nil), state.Findings...)
		request.PreparedAgentTask = ""
	}
	if stage.Prepare != nil {
		var err error
		request, err = stage.Prepare(ctx, request)
		if err != nil {
			return &contracts.StageReport{Stage: stage.Name()}, err
		}
	}
	result, err := stage.Dispatcher.Dispatch(ctx, request)
	state.Result = &result
	if err == nil && len(result.FinalDetectorStates) > 0 {
		updateStageFindings(state, ToolResponse{Findings: result.UnresolvedFindings, DetectorStates: result.FinalDetectorStates})
	}
	// A responder report never proves clearance. The controller still verifies it.
	return &contracts.StageReport{Stage: stage.Name()}, err
}

func (state *IncidentState) refreshUnexplained() {
	explained := map[string]bool{}
	for _, match := range state.Coverage.GetMatches() {
		for _, fp := range match.ExplainedFindingFingerprints {
			explained[fp] = true
		}
	}
	state.UnexplainedFindings = nil
	for _, finding := range state.Findings {
		if !explained[FindingFingerprint(finding)] {
			state.UnexplainedFindings = append(state.UnexplainedFindings, finding)
		}
	}
}
