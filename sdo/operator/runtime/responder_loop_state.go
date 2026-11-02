package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// applicationEvidence stores historical executor facts, never prepared handles.
// Pending is retained separately because it is not part of PlanApplication.
type applicationEvidence struct {
	Receipt   json.RawMessage
	Pending   bool
	PreImages []PlanPreImage
	Patches   []PlanPatch
}

// recordedRepairEvidence keeps controller-only ownership out of responder
// feedback while preserving it across the durable checkpoint boundary.
type recordedRepairEvidence struct {
	continuationRepair
	Owners []ResourceOwners
}

// responderToolState retains limits and revision-bound outcomes, never prepared
// plans or mutation/rollback authority. Immutable completed receipts are kept
// independently of continuation feedback. Legacy execution fields in version 1
// journals are deliberately ignored.
type responderToolState struct {
	ApplicationBindings map[string][]recoveryBinding
	StageProgress       map[string]responseStageProgress
	PendingWrites       map[string]pendingMitigation
	CompletedRepairs    []recordedRepairEvidence
	// ProtoJSON application facts, independent of execution authority.
	ApplicationEvidence []applicationEvidence
	Continuation        responderContinuation
	SessionID           string
	MenuSession         *contracts.MitigationSession
	MenuReports         []*contracts.MitigationReport
	MenuRepairs         []RepairActionReceipt
	MenuExecutions      map[string][]MenuRoundExecution
	Version             int
	IncidentID          string
	SourceCommit        string
	FailureSignals      []string
	Batches             int
	LoopOpen            bool
	ApplySeq            int
	MaxBatches          int
}

func (l *ResponderLoop) persistToolState(ctx context.Context) error {
	if l.saveToolState == nil {
		return nil
	}
	var evidence []applicationEvidence
	for _, application := range l.applicationEvidenceLocked() {
		projected, err := incidentResultContract(IncidentResult{PlanApplications: []RecordedPlanApplication{application}})
		if err != nil {
			return fmt.Errorf("project application evidence: %w", err)
		}
		encoded, err := protojson.Marshal(projected.PlanApplications[0])
		if err != nil {
			return fmt.Errorf("encode application evidence: %w", err)
		}
		evidence = append(evidence, applicationEvidence{Receipt: encoded, Pending: application.Pending, PreImages: application.Result.PreImages, Patches: application.Result.Patches})
	}
	var repairs []recordedRepairEvidence
	for _, repair := range l.applicationRepairsLocked() {
		repairs = append(repairs, recordedRepairEvidence{continuationRepair: repair, Owners: repair.Receipt.Owners})
	}
	payload, err := json.Marshal(responderToolState{ApplicationBindings: l.applicationBindings, ApplicationEvidence: evidence, StageProgress: l.stageProgress, PendingWrites: l.pendingWrites, CompletedRepairs: repairs, Continuation: l.continuation, Version: 2, SessionID: l.sessionID, MenuSession: l.menuSession, MenuReports: l.menuReports, MenuRepairs: l.menuRepairs, MenuExecutions: l.menuExecutions, IncidentID: l.Request.IncidentID, SourceCommit: l.Request.SourceCommit, FailureSignals: l.failureSignals, Batches: l.batches, LoopOpen: l.loopOpen, ApplySeq: l.applySeq, MaxBatches: l.MaxBatches})
	if err != nil {
		return fmt.Errorf("encode responder attempt state: %w", err)
	}
	if err = l.saveToolState(ctx, payload); err != nil {
		return fmt.Errorf("persist responder attempt state: %w", err)
	}
	return nil
}
func (l *ResponderLoop) restoreToolSnapshot(payload []byte) error {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	var state responderToolState
	if err := json.Unmarshal(payload, &state); err != nil {
		return fmt.Errorf("decode responder attempt state: %w", err)
	}
	if (state.Version != 1 && state.Version != 2) || state.IncidentID != l.Request.IncidentID || state.SourceCommit != l.Request.SourceCommit {
		return fmt.Errorf("responder attempt state does not match incident/revision")
	}
	if state.Continuation.Attempts < 0 || (state.Continuation.Attempts > 0) != (state.Continuation.Result != nil) || (state.Continuation.Done && state.Continuation.Result == nil) {
		return fmt.Errorf("invalid responder continuation state")
	}
	if state.Continuation.Result != nil && state.Continuation.Result.GetIncidentId() != l.Request.IncidentID {
		return fmt.Errorf("responder continuation belongs to another incident")
	}
	if state.Batches < 0 || state.ApplySeq < 0 || state.MaxBatches < 0 {
		return fmt.Errorf("invalid responder attempt budget")
	}
	if state.MenuSession != nil && (state.MenuSession.IncidentId != l.Request.IncidentID || state.MenuSession.SessionId != state.SessionID) {
		return fmt.Errorf("responder menu session does not match incident")
	}
	var archived []RecordedPlanApplication
	seen := map[string]bool{}
	for _, entry := range state.ApplicationEvidence {
		var receipt contracts.PlanApplication
		if err := protojson.Unmarshal(entry.Receipt, &receipt); err != nil {
			return fmt.Errorf("decode application evidence: %w", err)
		}
		if receipt.ApplicationId == "" || seen[receipt.ApplicationId] {
			return fmt.Errorf("invalid application evidence identity")
		}
		seen[receipt.ApplicationId] = true
		application := planApplicationState(&receipt)
		application.Pending = entry.Pending
		application.Result.PreImages = entry.PreImages
		application.Result.Patches = entry.Patches
		archived = append(archived, application)
	}
	l.archivedApplications = archived
	if state.SessionID != "" {
		l.sessionID = state.SessionID
	}
	l.stageProgress = state.StageProgress
	l.applicationBindings = state.ApplicationBindings
	l.restoreTerminalStagePlanErrors()
	l.pendingWrites = state.PendingWrites
	l.completedRepairs = nil
	for _, stored := range state.CompletedRepairs {
		repair := stored.continuationRepair
		repair.Receipt.Owners = stored.Owners
		l.completedRepairs = append(l.completedRepairs, repair)
	}
	l.continuation = state.Continuation
	l.menuSession = state.MenuSession
	l.menuReports = state.MenuReports
	l.menuRepairs = state.MenuRepairs
	l.menuExecutions = state.MenuExecutions
	l.batches = state.Batches
	l.loopOpen = state.LoopOpen
	l.applySeq = max(state.ApplySeq, state.Batches)
	if state.MaxBatches > 0 && state.MaxBatches < l.MaxBatches {
		l.MaxBatches = state.MaxBatches
	}
	l.failureSignals = state.FailureSignals
	return nil
}
