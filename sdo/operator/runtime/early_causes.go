package runtime

import "slices"

// Reconcile only controller-synthesized early causes with the final report.
// An authored cause takes ownership of each resource it names; independent
// opening evidence follows that owner. Unreported repaired resources retain
// their synthesized cause. Overlapping authored claims keep the ordinary
// validator's ambiguity checks rather than gaining authority through merging.
func reconcileEarlyCauses(synthesized, reported []ConfirmedRootCause) []ConfirmedRootCause {
	result := make([]ConfirmedRootCause, len(reported))
	for index, cause := range reported {
		result[index] = cause
		result[index].Evidence = slices.Clone(cause.Evidence)
		result[index].ExplainedDetectors = slices.Clone(cause.ExplainedDetectors)
		result[index].StaticContext = slices.Clone(cause.StaticContext)
	}
	for _, cause := range synthesized {
		matched := false
		for index := range result {
			for _, ref := range cause.Resources {
				for _, owner := range result[index].Resources {
					if ref.Kind == owner.Kind && ref.Namespace == owner.Namespace && ref.Name == owner.Name && (ref.APIVersion == "" || owner.APIVersion == "" || ref.APIVersion == owner.APIVersion) {
						mergeEarlyCauseEvidence(&result[index], cause)
						matched = true
					}
				}
			}
		}
		if !matched {
			result = append(result, cause)
		}
	}
	return result
}

func mergeEarlyCauseEvidence(owner *ConfirmedRootCause, prior ConfirmedRootCause) {
	for _, evidence := range prior.Evidence {
		if !slices.Contains(owner.Evidence, evidence) {
			owner.Evidence = append(owner.Evidence, evidence)
		}
	}
	for _, detector := range prior.ExplainedDetectors {
		if !slices.Contains(owner.ExplainedDetectors, detector) {
			owner.ExplainedDetectors = append(owner.ExplainedDetectors, detector)
		}
	}
	for _, context := range prior.StaticContext {
		if !slices.Contains(owner.StaticContext, context) {
			owner.StaticContext = append(owner.StaticContext, context)
		}
	}
}
