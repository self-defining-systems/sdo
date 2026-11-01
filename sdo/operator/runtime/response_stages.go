package runtime

import (
	"context"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// ResponseStage acts on the incident's current evidence and reports what it
// did. It neither chooses a later stage nor grants authority to close the
// incident: remaining findings are always re-observed by the controller.
type ResponseStage interface {
	Name() string
	Run(context.Context, *IncidentState) (*contracts.StageReport, error)
}

// IncidentState is local to one ordered response attempt. Request keeps the
// opening evidence; Findings is what is still open after the stages so far.
type IncidentState struct {
	Request     IncidentRequest
	Findings    []sdk.Finding
	PriorStages []*contracts.StageReport
	// DetectorStates and Healthy come from the latest independent observation
	// an automatic stage made after a write. Healthy is true only when that
	// observation completed without error and found nothing open.
	DetectorStates []DetectorEvaluation
	Healthy        bool
	// Result is set by the agent stage.
	Result *IncidentResult
	// RepairActions and ProgramToolCalls are the evidence automatic stages
	// produced through the ordinary plan/apply boundary.
	RepairActions    []RepairActionReceipt
	ProgramToolCalls []ToolCall
}

// RunResponseStages runs stages in order and stops once no findings remain.
// Every attempted stage leaves a report, including on error, so a later worker
// never infers success from silence.
func RunResponseStages(ctx context.Context, state *IncidentState, stages []ResponseStage) error {
	for _, stage := range stages {
		if len(state.Findings) == 0 {
			break
		}
		before := findingFingerprints(state.Findings)
		report, err := stage.Run(ctx, state)
		if report == nil {
			report = &contracts.StageReport{}
		}
		report.Stage = stage.Name()
		remaining := map[string]bool{}
		report.RemainingFindingFingerprints = nil
		for _, fingerprint := range findingFingerprints(state.Findings) {
			remaining[fingerprint] = true
			report.RemainingFindingFingerprints = append(report.RemainingFindingFingerprints, fingerprint)
		}
		report.ClearedFindingFingerprints = nil
		for _, fingerprint := range before {
			if !remaining[fingerprint] {
				report.ClearedFindingFingerprints = append(report.ClearedFindingFingerprints, fingerprint)
			}
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

func findingFingerprints(findings []sdk.Finding) []string {
	var result []string
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			result = append(result, FindingFingerprint(finding))
		}
	}
	sort.Strings(result)
	return result
}

func cloneStageReports(reports []*contracts.StageReport) []*contracts.StageReport {
	var result []*contracts.StageReport
	for _, report := range reports {
		result = append(result, proto.Clone(report).(*contracts.StageReport))
	}
	return result
}
