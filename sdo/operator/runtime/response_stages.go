package runtime

import (
	"context"
	"fmt"

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
}

// RunResponseStages stops when no findings remain. Every attempted stage has a
// report, including errors, so subsequent workers never infer success by silence.
func RunResponseStages(ctx context.Context, state *IncidentState, stages []ResponseStage) error {
	for _, stage := range stages {
		if len(state.Findings) == 0 {
			break
		}
		report, err := stage.Run(ctx, state)
		if report == nil {
			report = &contracts.StageReport{}
		}
		report.Stage = stage.Name()
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

func cloneStageReports(reports []*contracts.StageReport) []*contracts.StageReport {
	var result []*contracts.StageReport
	for _, report := range reports {
		result = append(result, proto.Clone(report).(*contracts.StageReport))
	}
	return result
}
