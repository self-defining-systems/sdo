package validate

import (
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// VerifiedReflectionCauses selects the causes whose repair steps may be saved.
// On a closure verified healthy (success) a cause needs only a recorded
// successful repair touching its resources. On any other outcome a cause needs
// a recorded mutation and fresh clearance of its opening findings, including
// symptoms that recovered without being written, plus controller eligibility
// and exclusive ownership of its resources.
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
		var verified bool
		if outcome.GetClassification() == "success" {
			// Closure verified healthy is the whole precondition: the recorded
			// repair steps are saved, and the repeat's act-verify loop checks
			// them again. No per-cause clearance or ownership proof is needed.
			verified = eligible && repairRecorded(outcome, cause.GetResources())
		} else {
			verified = eligible && RepairedCauseObserved(outcome, cause.GetResources())
			for _, resource := range cause.GetResources() {
				verified = verified && owners[[3]string{resource.GetKind(), resource.GetNamespace(), resource.GetName()}] == 1
			}
		}
		if verified {
			result = append(result, work)
		}
	}

	for index, cause := range outcome.GetUndetectedCauses() {
		if undetectedCauseValid(outcome, cause) {
			result = append(result, &contracts.ReflectionCauseWork{CauseIndex: uint32(len(outcome.GetConfirmedRootCauses()) + index), Resources: []*contracts.ObjectRef{cause.GetResource()}, LearningBasis: "observed_transition"})
		}
	}
	return result
}

// RepairedCauseObserved checks the shared evidence contract independently of
// aggregate classification or eligibility projection: a timed mutation and
// opening findings for every target/symptom cleared after the last cause write.
// Written targets need primary findings; symptoms may be related observations.
// On a success outcome, a symptom no opening finding observed is unobserved
// context, not failed proof; a symptom that was observed must still clear.
// A written target no opening finding observed is covered by its cause when
// another resource of the same cause is the primary resource of an opening
// finding that cleared after the last cause write. Opening findings omit findings that activate
// after launch, so without global health closure every resource needs its
// own cleared finding.
// Callers still enforce ownership, trusted action provenance and eligibility.
func RepairedCauseObserved(outcome *contracts.OutcomeRecord, resources []*contracts.ObjectRef) bool {
	repaired, after, valid := causeRepairs(outcome, resources)
	if !valid || len(repaired) == 0 {
		return false
	}
	healthClosed := outcome.GetClassification() == "success"
	for _, resource := range resources {
		primary := slices.ContainsFunc(repaired, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) })
		if healthClosed && !openingObserved(outcome, resource) {
			if !primary || causeFindingCleared(outcome, resources, after) {
				continue
			}
			return false
		}
		if !clearedOpenFinding(outcome, resource, after, primary) {
			return false
		}
	}
	return true
}

// causeFindingCleared reports whether an opening finding whose primary
// resource is in the cause cleared after the last cause write. A finding that
// names a cause resource only as related belongs to another cause and never
// covers this one.
func causeFindingCleared(outcome *contracts.OutcomeRecord, resources []*contracts.ObjectRef, after time.Time) bool {
	return slices.ContainsFunc(resources, func(resource *contracts.ObjectRef) bool {
		return slices.ContainsFunc(outcome.GetFindings(), func(f *contracts.Finding) bool {
			return f.GetStatus() == "active" && equalCauseResource(f.GetPrimaryResource(), resource)
		}) && clearedOpenFinding(outcome, resource, after, true)
	})
}

// openingObserved reports whether any opening finding named the resource as
// its primary or related object.
func openingObserved(outcome *contracts.OutcomeRecord, resource *contracts.ObjectRef) bool {
	return slices.ContainsFunc(outcome.GetFindings(), func(f *contracts.Finding) bool {
		return f.GetStatus() == "active" && (equalCauseResource(f.GetPrimaryResource(), resource) || slices.ContainsFunc(f.GetRelatedResources(), func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, resource) }))
	})
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
		if len(OwnedCauseWrites(action, resources)) > 0 || unchainedControlledWrite(action, resources) {
			// A write to an object a cause resource controls is part of that
			// cause's repair: its findings must clear after this write too.
			// A controlled object no open phase observed (e.g. a ReplicaSet a
			// rollout created after open) has no chain, so it bounds every cause.
			if action.GetStartedAt() == nil || !action.GetStartedAt().IsValid() || action.GetCompletedAt() == nil || !action.GetCompletedAt().IsValid() || action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime()) {
				return nil, time.Time{}, false
			}
			if action.GetCompletedAt().AsTime().After(after) {
				after = action.GetCompletedAt().AsTime()
			}
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

// OwnedCauseWrites returns the action's written resources that are not cause
// resources themselves but whose recorded controller owner chain (read from
// the live object at write time) reaches one. Names never establish ownership.
func OwnedCauseWrites(action *contracts.RepairActionReceipt, resources []*contracts.ObjectRef) []*contracts.ObjectRef {
	var owned []*contracts.ObjectRef
	for _, written := range action.GetResources() {
		if slices.ContainsFunc(resources, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, written) }) {
			continue
		}
		for _, owners := range action.GetOwners() {
			if equalCauseResource(owners.GetResource(), written) && slices.ContainsFunc(owners.GetOwnerChain(), func(owner *contracts.ObjectRef) bool {
				return slices.ContainsFunc(resources, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, owner) })
			}) {
				owned = append(owned, written)
				break
			}
		}
	}
	return owned
}

// unchainedControlledWrite reports a write to a Pod, ReplicaSet or Job that
// is not a cause resource and has no recorded owner chain. Its owner is
// unknown, so it conservatively counts as part of every cause's repair.
func unchainedControlledWrite(action *contracts.RepairActionReceipt, resources []*contracts.ObjectRef) bool {
	return slices.ContainsFunc(action.GetResources(), func(written *contracts.ObjectRef) bool {
		switch written.GetKind() {
		case "Pod", "ReplicaSet", "Job":
		default:
			return false
		}
		return !slices.ContainsFunc(resources, func(ref *contracts.ObjectRef) bool { return equalCauseResource(ref, written) }) &&
			!slices.ContainsFunc(action.GetOwners(), func(owners *contracts.ResourceOwners) bool {
				return len(owners.GetOwnerChain()) > 0 && equalCauseResource(owners.GetResource(), written)
			})
	})
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

// repairRecorded reports a valid, timed, successful repair action that wrote a
// cause resource or an object a cause resource controls.
func repairRecorded(outcome *contracts.OutcomeRecord, resources []*contracts.ObjectRef) bool {
	repaired, after, valid := causeRepairs(outcome, resources)
	return valid && (len(repaired) > 0 || !after.IsZero())
}
