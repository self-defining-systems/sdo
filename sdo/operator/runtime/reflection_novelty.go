package runtime

import (
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"slices"
)

// coveredRepeatCandidate is deliberately only an admission filter for a
// broker-owned no-change Check, never publication authority. Unknown evidence
// keeps learning available. The shared Check still rejects hand repairs and
// stale/partial stage applications from the committed controller outcome.
func coveredRepeatCandidate(outcome *contracts.OutcomeRecord) bool {
	if outcome.GetClassification() != "success" || len(outcome.GetFindings()) == 0 || outcome.RepairCommit != nil || outcome.GetResponseLoopExhausted() || len(outcome.GetProgramFailureSignals()) > 0 || len(outcome.GetUndetectedCauses()) > 0 || len(outcome.GetDisabledSetupDetectors()) > 0 || outcome.GetStatePersistenceFailure() != nil {
		return false
	}
	applied := map[string][]*contracts.PlanApplication{}
	for _, app := range outcome.GetPlanApplications() {
		if !app.GetApplied() || app.GetDryRun() || app.GetRolledBack() || app.GetError() != "" || app.GetRollbackError() != "" || app.GetRolloutPending() != nil || !app.GetVerificationCompleted() || !app.GetVerificationPassed() || app.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
			return false
		}
		for _, id := range app.GetProgramIds() {
			applied[id] = append(applied[id], app)
		}
	}
	covered := map[string]bool{}
	for _, match := range outcome.GetPlaybookCoverage().GetMatches() {
		if match.GetProgramRevision() == "" || !slices.ContainsFunc(applied[match.GetPlaybookId()], func(app *contracts.PlanApplication) bool { return applicationCoversMatch(outcome, app, match) }) {
			continue
		}
		for _, fingerprint := range match.GetExplainedFindingFingerprints() {
			covered[fingerprint] = true
		}
	}
	for _, finding := range outcome.GetFindings() {
		// SDK findings may omit an explicit fingerprint. Coverage uses the shared
		// canonical identity in that case; use the same owner on this edge too.
		fingerprint := FindingFingerprint(findingState(finding))
		if fingerprint == "" || !covered[fingerprint] {
			return false
		}
	}
	return true
}

// Coverage is binding- and revision-specific. A successful peer binding of the
// same program cannot cover this finding. Reuse the independently recorded
// post-apply evidence that also drives program trust.
func applicationCoversMatch(outcome *contracts.OutcomeRecord, app *contracts.PlanApplication, match *contracts.PlaybookMatch) bool {
	id, revision := match.GetPlaybookId(), match.GetProgramRevision()
	if !boundPostApplyNoMatch(outcome, app, id, revision) {
		return false
	}
	bindings := programBindingsState(match.GetBindings())
	for _, bound := range appliedPlanBindings(outcome, app, id) {
		if bindings.Equal(bound) {
			return true
		}
	}
	for _, stage := range outcome.GetPriorStages() {
		for _, attempt := range stage.GetMitigations() {
			receipt := attempt.GetApplyReceipt()
			if attempt.GetPlaybookId() == id && attempt.GetProgramRevision() == revision && receipt.GetApplicationId() == app.GetApplicationId() && proto.Equal(receipt.GetPlan(), app.GetPlan()) && bindings.Equal(programBindingsState(attempt.GetBindings())) {
				return true
			}
		}
	}
	return false
}

// A generic-first incident needs the worker to assess cause visibility before
// deciding no change. The worker owns the measured-delay hint; this conservative
// admission filter adds no publication rule and no duplicate timing threshold.
func genericHealthDetectedFirst(closure *contracts.IncidentClosure) bool {
	var first *contracts.DetectorTimelineEntry
	for _, entry := range closure.GetDetectorTimeline() {
		if entry.GetFirstActivatedAt() == nil {
			continue
		}
		if first == nil || entry.GetFirstActivatedAt().AsTime().Before(first.GetFirstActivatedAt().AsTime()) {
			first = entry
		}
	}
	if first == nil || first.GetDetectorClass() != "health" {
		return false
	}
	for _, entry := range closure.GetDetectorTimeline() {
		if entry.GetFirstActivatedAt() != nil && entry.GetFirstActivatedAt().AsTime().Equal(first.GetFirstActivatedAt().AsTime()) && entry.GetDetectorClass() != "health" {
			return false
		}
	}
	return true
}
