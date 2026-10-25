package validate

import (
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// VerifiedReflectionCauses selects controller evidence, never aggregate success
// or the responder's cause claim. Each cause resource must have a successful
// change followed by clearance of a finding that was present at incident open.
// Partial outcomes must additionally carry controller eligibility for the cause.
func VerifiedReflectionCauses(outcome *contracts.OutcomeRecord) []*contracts.ReflectionCauseWork {
	owners := map[[3]string]int{}
	for _, cause := range outcome.GetConfirmedRootCauses() {
		seen := map[[3]string]bool{}
		for _, ref := range cause.GetResources() {
			key := [3]string{ref.GetKind(), ref.GetNamespace(), ref.GetName()}
			if !seen[key] {
				owners[key]++
				seen[key] = true
			}
		}
	}
	var result []*contracts.ReflectionCauseWork
	for index, cause := range outcome.GetConfirmedRootCauses() {
		work := &contracts.ReflectionCauseWork{CauseIndex: uint32(index), Resources: cause.GetResources()}
		eligible := outcome.GetClassification() == "success"
		for _, candidate := range outcome.GetReflectionEligibility().GetCauses() {
			if candidate.GetCauseIndex() == uint32(index) {
				eligible = true
				work.Items = candidate.GetItems()
			}
		}
		verified := eligible && len(cause.GetResources()) > 0
		for _, resource := range cause.GetResources() {
			verified = verified && owners[[3]string{resource.GetKind(), resource.GetNamespace(), resource.GetName()}] == 1 && repairedOpenFinding(outcome, resource)
		}
		if verified {
			result = append(result, work)
		}
	}
	return result
}

func equalCauseResource(a, b *contracts.ObjectRef) bool {
	return a.GetKind() != "" && a.GetName() != "" && a.GetKind() == b.GetKind() && a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName() && (a.GetApiVersion() == "" || b.GetApiVersion() == "" || a.GetApiVersion() == b.GetApiVersion())
}

func repairedOpenFinding(outcome *contracts.OutcomeRecord, resource *contracts.ObjectRef) bool {
	for _, action := range outcome.GetRepairActions() {
		if !action.GetSuccess() || action.GetStartedAt() == nil || !action.GetStartedAt().IsValid() || action.GetCompletedAt() == nil || !action.GetCompletedAt().IsValid() || action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) {
			continue
		}
		touched := slices.ContainsFunc(action.GetResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) })
		if !touched {
			continue
		}
		latestChange := true
		for _, peer := range outcome.GetRepairActions() {
			if !peer.GetSuccess() || !slices.ContainsFunc(peer.GetResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) }) {
				continue
			}
			if peer.GetCompletedAt() == nil || !peer.GetCompletedAt().IsValid() || peer.GetCompletedAt().AsTime().After(action.GetCompletedAt().AsTime()) {
				latestChange = false
			}
		}
		if !latestChange {
			continue
		}
		for _, finding := range outcome.GetFindings() {
			if finding.GetStatus() != "active" || finding.GetDetectorId() == "" || !equalCauseResource(finding.GetPrimaryResource(), resource) {
				continue
			}
			// Use the last controller evaluation for this detector. Missing,
			// stale, failed, or still firing evidence never establishes repair.
			var latest *contracts.DetectorEvaluation
			evaluations := append(append([]*contracts.DetectorEvaluation{}, outcome.GetDetectorHistory()...), outcome.GetFinalHealthDetectorState()...)
			for _, evaluation := range evaluations {
				if evaluation.GetDetectorId() == finding.GetDetectorId() && evaluation.GetEvaluatedAt() != nil && evaluation.GetEvaluatedAt().IsValid() && (latest == nil || !evaluation.GetEvaluatedAt().AsTime().Before(latest.GetEvaluatedAt().AsTime())) {
					latest = evaluation
				}
			}
			if latest == nil || latest.GetError() != "" || latest.GetEvaluatedAt().AsTime().Before(action.GetCompletedAt().AsTime()) {
				continue
			}
			if latest.GetStatus() == "clear" && len(latest.GetFingerprints()) == 0 {
				return true
			}
			if latest.GetStatus() == "firing" && finding.GetFingerprint() != "" && len(latest.GetFingerprints()) > 0 && !slices.Contains(latest.GetFingerprints(), finding.GetFingerprint()) {
				return true
			}
		}
	}
	return false
}
