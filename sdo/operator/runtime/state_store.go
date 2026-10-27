package runtime

import (
	"context"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"time"
)

// RuntimeStateVersion comes from the durable generated contract.
var RuntimeStateVersion = wire.StringConst(&contracts.ControllerState{}, "version")

var ErrStateConflict = lifecycleio.ErrStateConflict

// StateStore is injected into the controller driver; only the driver persists
// transitions. Production adapters are private to this subsystem.
type StateStore interface {
	Load(context.Context) (*contracts.ControllerState, string, error)
	Save(context.Context, *contracts.ControllerState, string) (string, error)
}

func validateRuntimeState(state *contracts.ControllerState) error {
	if state == nil {
		return fmt.Errorf("controller state is required")
	}
	return wire.Validate(state)
}

func cloneIncidentRequest(request *IncidentRequest) *IncidentRequest {
	if request == nil {
		return nil
	}
	copy := *request
	// Lists stay lists when empty: the responder's and the broker's schemas
	// reject null (a cold incident surfaces no playbook and no outcome).
	copy.Findings = append(make([]sdk.Finding, 0, len(request.Findings)), request.Findings...)
	copy.DriftEvents = append([]sdk.DriftEvent(nil), request.DriftEvents...)
	copy.NamespaceOnboarding = cloneNamespaceOnboarding(request.NamespaceOnboarding)
	copy.DisabledSetupDetectors = cloneDisabledSetupDetectors(request.DisabledSetupDetectors)
	copy.DetectorHistory = cloneEvaluations(request.DetectorHistory)
	copy.SurfacedPlaybooks = append(make([]SurfacedPlaybook, 0, len(request.SurfacedPlaybooks)), request.SurfacedPlaybooks...)
	copy.RelevantOutcomes = append(make([]PriorOutcomeEvidence, 0, len(request.RelevantOutcomes)), request.RelevantOutcomes...)
	copy.StateChanges = cloneStateChanges(request.StateChanges)
	copy.PlaybookCoverage = cloneCoverage(request.PlaybookCoverage)
	copy.PriorStages = cloneStageReports(request.PriorStages)
	return &copy
}

// cloneEvaluations copies detector evaluations; the list and each
// evaluation's fingerprints stay lists when empty, never null.
func cloneEvaluations(evaluations []DetectorEvaluation) []DetectorEvaluation {
	copied := make([]DetectorEvaluation, 0, len(evaluations))
	for _, evaluation := range evaluations {
		evaluation.Fingerprints = append(make([]string, 0, len(evaluation.Fingerprints)), evaluation.Fingerprints...)
		copied = append(copied, evaluation)
	}
	return copied
}

func cloneIncidentResult(result *IncidentResult) *IncidentResult {
	if result == nil {
		return nil
	}
	copy := *result
	copy.DriftReprompt = cloneDriftReprompt(result.DriftReprompt)
	copy.UnresolvedFindings = append([]sdk.Finding(nil), result.UnresolvedFindings...)
	copy.DriftEvents = driftEventsState(driftEventsContract(result.DriftEvents))
	copy.ConfirmedRootCauses = append(make([]ConfirmedRootCause, 0, len(result.ConfirmedRootCauses)), result.ConfirmedRootCauses...)
	for index := range copy.ConfirmedRootCauses {
		copy.ConfirmedRootCauses[index].Resources = append(
			make([]sdk.ObjectRef, 0, len(result.ConfirmedRootCauses[index].Resources)),
			result.ConfirmedRootCauses[index].Resources...,
		)
	}
	copy.AppliedPlaybooks = append(make([]AppliedPlaybook, 0, len(result.AppliedPlaybooks)), result.AppliedPlaybooks...)
	for index := range copy.AppliedPlaybooks {
		copy.AppliedPlaybooks[index].ScriptNames = append(
			make([]string, 0, len(result.AppliedPlaybooks[index].ScriptNames)), result.AppliedPlaybooks[index].ScriptNames...,
		)
		copy.AppliedPlaybooks[index].ParameterBindings = make(map[string]sdk.ObjectRef)
		for name, resource := range result.AppliedPlaybooks[index].ParameterBindings {
			copy.AppliedPlaybooks[index].ParameterBindings[name] = resource
		}
	}
	copy.RepairChanges = append(make([]string, 0, len(result.RepairChanges)), result.RepairChanges...)
	copy.RepairActions = append(
		make([]RepairActionReceipt, 0, len(result.RepairActions)), result.RepairActions...,
	)
	copy.FinalDetectorStates = cloneEvaluations(result.FinalDetectorStates)
	copy.ProposedArtifactIDs = append(
		make([]string, 0, len(result.ProposedArtifactIDs)), result.ProposedArtifactIDs...,
	)
	copy.VerificationEvidence = append(
		make([]VerificationEvidence, 0, len(result.VerificationEvidence)), result.VerificationEvidence...,
	)
	return &copy
}

func cloneIncidentClosure(closure *IncidentClosure) *IncidentClosure {
	if closure == nil {
		return nil
	}
	copy := *closure
	copy.StatePersistenceFailure = clonePersistenceFailure(closure.StatePersistenceFailure)
	copy.Request = *cloneIncidentRequest(&closure.Request)
	copy.Result = cloneIncidentResult(closure.Result)
	copy.FinalDetectorStates = cloneEvaluations(closure.FinalDetectorStates)
	copy.IncidentDetectorStates = cloneEvaluations(closure.IncidentDetectorStates)
	copy.PartialDetectorStates = cloneEvaluations(closure.PartialDetectorStates)
	copy.DetectorTimeline = cloneTimeline(closure.DetectorTimeline)
	copy.SourceCheckNotes = append([]string(nil), closure.SourceCheckNotes...)
	copy.FinalStateChanges = cloneStateChanges(closure.FinalStateChanges)
	if closure.HealthClearedAt != nil {
		clearedAt := *closure.HealthClearedAt
		copy.HealthClearedAt = &clearedAt
	}
	return &copy
}

// cloneStateChanges deep-copies a StateChanges so a published or persisted
// snapshot cannot be mutated through a shared slice. Changes stays a list
// when empty: the broker's and the responder's schemas reject null.
func cloneStateChanges(changes *StateChanges) *StateChanges {
	if changes == nil {
		return nil
	}
	copy := *changes
	copy.Changes = append(make([]StateChange, 0, len(changes.Changes)), changes.Changes...)
	copy.UnobservedKinds = append([]string(nil), changes.UnobservedKinds...)
	return &copy
}

func cloneClosureReceipt(receipt *ClosureReceipt) *ClosureReceipt {
	if receipt == nil {
		return nil
	}
	copy := *receipt
	return &copy
}

func cloneClearSince(since map[string]time.Time) map[string]time.Time {
	if len(since) == 0 {
		return nil
	}
	copy := make(map[string]time.Time, len(since))
	for id, at := range since {
		copy[id] = at
	}
	return copy
}
