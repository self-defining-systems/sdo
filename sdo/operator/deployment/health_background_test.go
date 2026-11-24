package deployment

import (
	"testing"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/requestmetrics"
)

// EKS series: 10.6.1 was held because request-metrics reported Sidekiq's background jobs failing (an entry only
// internal operations enter). Failing background jobs are clues, not failing user traffic (#1704): they never open
// an incident, and they never hold a head or a repair either. The finding stays visible as a coverage note.
func TestBackgroundJobFindingsNeverMakeAVerdictFiring(t *testing.T) {
	background := sdk.Finding{DetectorID: "request-metrics", RuleID: requestmetrics.RuleErrors, Status: sdk.FindingActive,
		Summary: "operation run-background-jobs: 13 of 22 requests failed", Metadata: map[string]any{requestmetrics.MetaBackground: true}}
	got := verdictOf(healthEvaluationOf(evaluation.Verdict{Status: evaluation.Clear, Findings: []sdk.Finding{background}, ClueFindings: []int{0}}))
	if got.Outcome != lifecycle.VerificationClear || len(got.Detectors) != 0 || len(got.CoverageNotes) != 1 {
		t.Fatalf("a background-job finding must be a coverage note of a clear verdict: %+v", got)
	}
	user := background
	user.Metadata = nil
	held := verdictOf(healthEvaluationOf(evaluation.Verdict{Status: evaluation.Firing, Findings: []sdk.Finding{user}}))
	if held.Outcome != lifecycle.VerificationSourceFault || len(held.Detectors) != 1 {
		t.Fatalf("a user-traffic finding must still hold: %+v", held)
	}
}
