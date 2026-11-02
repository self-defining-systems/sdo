package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func driftEventsState(items []*contracts.DriftEvent) []sdk.DriftEvent {
	var events []sdk.DriftEvent
	for _, item := range items {
		event := sdk.DriftEvent{Ref: objectRefState(item.GetRef()), Source: item.GetSource(), Manager: item.GetManager(), Operation: item.GetOperation(), ObservedAt: controllerTime(item.GetObservedAt()), Classification: sdk.DriftClassification(item.GetClassification()), ProgramID: item.GetProgramId(), InvestigationVerdict: item.GetInvestigationVerdict(), InvestigationReason: item.GetInvestigationReason(), InvestigationStatus: item.GetInvestigationStatus(), InvestigationCaptureID: item.GetInvestigationCaptureId()}
		if item.GetManagerAt() != nil {
			at := controllerTime(item.GetManagerAt())
			event.ManagerAt = &at
		}
		for _, field := range item.GetFields() {
			event.Fields = append(event.Fields, sdk.DriftField{Path: field.GetPath(), Old: field.GetOld(), New: field.GetNew(), Source: field.GetSource()})
		}
		for _, evidence := range item.GetInvestigationEvidence() {
			event.InvestigationEvidence = append(event.InvestigationEvidence, sdk.DriftEvidence{Kind: evidence.GetKind(), Observation: evidence.GetObservation()})
		}
		events = append(events, event)
	}
	return events
}
func adoptionContract(s OwnershipAdoptionStatus) *contracts.OwnershipAdoptionStatus {
	return &contracts.OwnershipAdoptionStatus{SourceCommit: s.SourceCommit, AdoptedObjects: int32(s.AdoptedObjects), ConflictsTakenOver: int32(s.ConflictsTakenOver), FromManagers: append([]string(nil), s.FromManagers...), LastError: s.LastError, SkippedReason: s.SkippedReason, RetryAt: optionalTimestamp(s.RetryAt), Failures: int32(s.Failures), AcceptedObjects: append([]string(nil), s.AcceptedObjects...), UnadoptedObjects: append([]string(nil), s.UnadoptedObjects...)}
}
func adoptionState(s *contracts.OwnershipAdoptionStatus) OwnershipAdoptionStatus {
	return OwnershipAdoptionStatus{SourceCommit: s.GetSourceCommit(), AdoptedObjects: int(s.GetAdoptedObjects()), ConflictsTakenOver: int(s.GetConflictsTakenOver()), FromManagers: append([]string(nil), s.GetFromManagers()...), LastError: s.GetLastError(), SkippedReason: s.GetSkippedReason(), RetryAt: controllerTime(s.GetRetryAt()), Failures: int(s.GetFailures()), AcceptedObjects: append([]string(nil), s.GetAcceptedObjects()...), UnadoptedObjects: append([]string(nil), s.GetUnadoptedObjects()...)}
}
