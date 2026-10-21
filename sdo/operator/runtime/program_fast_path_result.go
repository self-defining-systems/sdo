package runtime

import (
	"time"

	"sdo.dev/operator/sdk"
)

func fastPathResult(request IncidentRequest, loop *ResponderLoop, attempt FastPathAttempt, started time.Time) IncidentResult {
	completed := time.Now().UTC()
	result := IncidentResult{
		SchemaVersion: ProtocolSchemaVersion, IncidentID: request.IncidentID, Status: IncidentCompleted,
		Timing:              TimingMetrics{StartedAt: started, CompletedAt: completed},
		FinalDetectorStates: append([]DetectorEvaluation(nil), attempt.Verification.DetectorStates...),
		ProgramRuns:         loop.ProgramRuns(), PlanApplications: loop.PlanApplications(), ProgramFailureSignals: loop.FailureSignals(),
		ResolutionPath: "fast_path", FastPathPrograms: append([]string(nil), attempt.Programs...),
		FastPathRelationCoveredFindings: append([]RelationCoveredFinding(nil), attempt.RelationCoveredFindings...),
		VerificationEvidence:            []VerificationEvidence{{Name: "trusted program diagnosis and health", Passed: true, Details: attempt.Summary, ObservedAt: completed}},
	}
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, nil)
	for _, finding := range attempt.Findings {
		result.ConfirmedRootCauses = append(result.ConfirmedRootCauses, ConfirmedRootCause{
			Summary: finding.Summary, Resources: []sdk.ObjectRef{finding.PrimaryResource},
			Evidence:           []RootCauseEvidence{{Kind: "detector-finding", Source: finding.DetectorID, Observation: finding.Evidence}},
			ExplainedDetectors: []string{finding.DetectorID},
		})
	}
	for _, id := range attempt.Programs {
		result.AppliedPlaybooks = append(result.AppliedPlaybooks, AppliedPlaybook{PlaybookID: id})
	}
	return result
}
