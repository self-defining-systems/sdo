package validate

import (
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// VerifiedReflectionCauses selects controller evidence, never aggregate success
// or the responder's cause claim. A cause needs a recorded mutation and fresh
// clearance of its opening findings, including symptoms that recovered without
// being written. Every written resource remains bound to an opening finding.
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
		verified := eligible && RepairedCauseObserved(outcome, cause.GetResources())
		for _, resource := range cause.GetResources() {
			verified = verified && owners[[3]string{resource.GetKind(), resource.GetNamespace(), resource.GetName()}] == 1
		}
		if verified {
			result = append(result, work)
		}
	}
	return result
}

// RepairedCauseObserved checks the shared evidence contract independently of
// aggregate classification or eligibility projection: a timed mutation and
// opening findings for every target/symptom cleared after the last cause write.
// Written targets need primary findings; symptoms may be related observations.
// Callers still enforce ownership, trusted action provenance and eligibility.
func RepairedCauseObserved(outcome *contracts.OutcomeRecord, resources []*contracts.ObjectRef) bool {
	repaired, after, valid := causeRepairs(outcome, resources)
	if !valid || len(repaired) == 0 {
		return false
	}
	for _, resource := range resources {
		primary := slices.ContainsFunc(repaired, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) })
		if !clearedOpenFinding(outcome, resource, after, primary) {
			return false
		}
	}
	return true
}

func equalCauseResource(a, b *contracts.ObjectRef) bool {
	return a.GetKind() != "" && a.GetName() != "" && a.GetKind() == b.GetKind() && a.GetNamespace() == b.GetNamespace() && a.GetName() == b.GetName() && (a.GetApiVersion() == "" || b.GetApiVersion() == "" || a.GetApiVersion() == b.GetApiVersion())
}

// causeRepairs returns only mutation targets in this cause and the last repair
// boundary. Invalid successful receipts fail closed instead of hiding a write.
func causeRepairs(outcome *contracts.OutcomeRecord, resources []*contracts.ObjectRef) ([]*contracts.ObjectRef, time.Time, bool) {
	var repaired []*contracts.ObjectRef
	var after time.Time
	for _, action := range outcome.GetRepairActions() {
		if !action.GetSuccess() {
			continue
		}
		for _, resource := range resources {
			if !slices.ContainsFunc(action.GetResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) }) {
				continue
			}
			if action.GetStartedAt() == nil || !action.GetStartedAt().IsValid() || action.GetCompletedAt() == nil || !action.GetCompletedAt().IsValid() || action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) {
				return nil, time.Time{}, false
			}
			if !slices.ContainsFunc(repaired, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) }) {
				repaired = append(repaired, resource)
			}
			if action.GetCompletedAt().AsTime().After(after) {
				after = action.GetCompletedAt().AsTime()
			}
		}
	}
	return repaired, after, true
}

func clearedOpenFinding(outcome *contracts.OutcomeRecord, resource *contracts.ObjectRef, after time.Time, primaryOnly bool) bool {
	// A primary symptom finding is the specific observation of that resource.
	// A cleared relation on a peer finding cannot override its unresolved state.
	primaryOnly = primaryOnly || slices.ContainsFunc(outcome.GetFindings(), func(f *contracts.Finding) bool {
		return f.GetStatus() == "active" && equalCauseResource(f.GetPrimaryResource(), resource)
	})
	for _, finding := range outcome.GetFindings() {
		observed := equalCauseResource(finding.GetPrimaryResource(), resource) || (!primaryOnly && slices.ContainsFunc(finding.GetRelatedResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) }))
		if finding.GetStatus() != "active" || finding.GetDetectorId() == "" || !observed {
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
		if latest == nil || latest.GetError() != "" || latest.GetEvaluatedAt().AsTime().Before(after) {
			continue
		}
		if latest.GetStatus() == "clear" && len(latest.GetFingerprints()) == 0 {
			return true
		}
		if latest.GetStatus() == "firing" && finding.GetFingerprint() != "" && len(latest.GetFingerprints()) > 0 && !slices.Contains(latest.GetFingerprints(), finding.GetFingerprint()) {
			return true
		}
	}
	return false
}
