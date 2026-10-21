package runtime

import (
	"sdo.dev/operator/outcome_derivation"
	"sdo.dev/operator/sdk"
)

func checkedDriftAssessments(request IncidentRequest, labels []sdk.DriftEvent) []sdk.DriftEvent {
	observed := append([]sdk.DriftEvent(nil), request.DriftEvents...)
	observed = append(observed, driftEventsForFindings(nil, request.Findings)...)
	return driftEventsState(outcome_derivation.CheckedDriftAssessments(driftEventsContract(observed), driftEventsContract(labels)))
}
