package outcome

import (
	"slices"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Values of ConfirmedRootCause.reach; "" is unset. TestReachValuesMatchProto
// keeps them equal to the rule in messages.proto.
const (
	ReachInScope    = "in-scope"
	ReachOutOfReach = "out-of-reach"
)

// OutOfReachConfirmed reports whether cause is an out-of-reach cause backed by
// controller evidence that predates every repair: a detector-finding evidence
// item whose source names (by detector ID, rule ID or "detector/rule") an
// active opening finding that observed one of the cause's resources. Opening
// findings are recorded when the incident opens, before the responder runs.
// A cause that some successful repair action wrote was within reach after all.
func OutOfReachConfirmed(cause *contracts.ConfirmedRootCause, opening []*contracts.Finding, repairs []*contracts.RepairActionReceipt) bool {
	if cause.GetReach() != ReachOutOfReach {
		return false
	}
	for _, action := range repairs {
		if action.GetSuccess() && slices.ContainsFunc(action.GetResources(), func(written *contracts.ObjectRef) bool { return causeResource(cause, written) }) {
			return false
		}
	}
	return slices.ContainsFunc(cause.GetEvidence(), func(evidence *contracts.RootCauseEvidence) bool {
		if evidence.GetKind() != "detector-finding" {
			return false
		}
		source := evidence.GetSource()
		return slices.ContainsFunc(opening, func(finding *contracts.Finding) bool {
			named := source == finding.GetDetectorId() || source == finding.GetRuleId() || source == finding.GetDetectorId()+"/"+finding.GetRuleId()
			observed := causeResource(cause, finding.GetPrimaryResource()) ||
				slices.ContainsFunc(finding.GetRelatedResources(), func(ref *contracts.ObjectRef) bool { return causeResource(cause, ref) })
			return finding.GetStatus() == "active" && finding.GetDetectorId() != "" && named && observed
		})
	})
}

// Escalation reports whether any of causes is OutOfReachConfirmed.
func Escalation(causes []*contracts.ConfirmedRootCause, opening []*contracts.Finding, repairs []*contracts.RepairActionReceipt) bool {
	return slices.ContainsFunc(causes, func(cause *contracts.ConfirmedRootCause) bool {
		return OutOfReachConfirmed(cause, opening, repairs)
	})
}

func causeResource(cause *contracts.ConfirmedRootCause, ref *contracts.ObjectRef) bool {
	return ref.GetKind() != "" && ref.GetName() != "" && slices.ContainsFunc(cause.GetResources(), func(resource *contracts.ObjectRef) bool {
		return resource.GetKind() == ref.GetKind() && resource.GetNamespace() == ref.GetNamespace() && resource.GetName() == ref.GetName()
	})
}
