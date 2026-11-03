package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

func wireState(t *testing.T) *contracts.ControllerState {
	now := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	closure := nativeClosure(IncidentWorkspace{IncidentID: "incident-wire", Worktree: "/work", BaseCommit: "base"})
	closure.Request.StateChanges = &StateChanges{BaselineAt: now, ObservedAt: now, Changes: []StateChange{}}
	closure.FinalStateChanges = &StateChanges{BaselineAt: now, ObservedAt: now, Changes: []StateChange{}}
	request := closure.Request
	finding := sdk.Finding{
		DetectorID: "health", RuleID: "rule", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, Summary: "s", Evidence: "e",
		PrimaryResource: sdk.ObjectRef{Kind: "Service", Name: "web"}, Fingerprint: "fp", Metadata: map[string]any{"count": float64(2)},
	}
	return &contracts.ControllerState{
		Version:            RuntimeStateVersion,
		SchedulerDeadlines: timestampsContract(map[string]time.Time{"health": now}),
		FindingStates: mustConvert(t, findingStatesContract, map[string]FindingState{
			"health/fp": {DetectorID: "health", Finding: finding, FiringCount: 2, Active: true, FirstSeenAt: now, Activations: 1},
		}),
		PendingBatch:    &contracts.BatcherState{Findings: mustConvert(t, findingsContract, []sdk.Finding{finding}), Deadlines: timestampsContract(map[string]time.Time{"health/fp": now}), Deadline: contractTimestamp(now)},
		History:         evaluationsContract([]DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring, Fingerprints: []string{"fp"}}}),
		IncidentOpen:    true,
		IncidentRequest: mustProject(t, incidentRequestContract, request),
		IncidentResult:  mustProject(t, incidentResultContract, *closure.Result),
		DispatchState:   contracts.DispatchState_DISPATCH_STATE_PENDING, IncidentFindingKeys: []string{"health/fp"},
		IncidentDetectedAt: contractTimestamp(now),
		PendingClosure:     mustProject(t, incidentClosureContract, closure),
		ClosureFailure:     &contracts.ClosureFailure{IncidentId: "incident-wire", Attempts: 1, MaxAttempts: 3, LastError: "x", FirstFailedAt: contractTimestamp(now), LastFailedAt: contractTimestamp(now)},
		IncidentView: mustProject(t, incidentViewContract, &IncidentView{
			IncidentID: "incident-wire", ObservedAt: now, BlockingDetectors: []string{"health"}, BlockingFindings: []sdk.Finding{finding},
		}),
	}
}
