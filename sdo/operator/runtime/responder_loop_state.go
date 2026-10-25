package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// responderToolState retains limits and revision-bound outcomes, never prepared
// plans or mutation/rollback authority. Immutable completed receipts are kept
// independently of continuation feedback. Version 1 journals are read only for these
// facts; their application fields are deliberately ignored.
type responderToolState struct {
	PendingWrites    map[string]pendingMitigation
	CompletedRepairs []continuationRepair
	Continuation     responderContinuation
	SessionID        string
	MenuSession      *contracts.MitigationSession
	MenuReports      []*contracts.MitigationReport
	MenuRepairs      []RepairActionReceipt
	MenuExecutions   map[string][]MenuRoundExecution
	Version          int
	IncidentID       string
	SourceCommit     string
	FailureSignals   []string
	Batches          int
	MaxBatches       int
}

func (l *ResponderLoop) persistToolState(ctx context.Context) error {
	if l.saveToolState == nil {
		return nil
	}
	payload, err := json.Marshal(responderToolState{PendingWrites: l.pendingWrites, CompletedRepairs: l.applicationRepairsLocked(), Continuation: l.continuation, Version: 2, SessionID: l.sessionID, MenuSession: l.menuSession, MenuReports: l.menuReports, MenuRepairs: l.menuRepairs, MenuExecutions: l.menuExecutions, IncidentID: l.Request.IncidentID, SourceCommit: l.Request.SourceCommit, FailureSignals: l.failureSignals, Batches: l.batches, MaxBatches: l.MaxBatches})
	if err != nil {
		return fmt.Errorf("encode responder attempt state: %w", err)
	}
	if err = l.saveToolState(ctx, payload); err != nil {
		return fmt.Errorf("persist responder attempt state: %w", err)
	}
	return nil
}
func (l *ResponderLoop) restoreToolSnapshot(payload []byte) error {
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
	if state.Batches < 0 || state.MaxBatches < 0 {
		return fmt.Errorf("invalid responder attempt budget")
	}
	if state.MenuSession != nil && (state.MenuSession.IncidentId != l.Request.IncidentID || state.MenuSession.SessionId != state.SessionID) {
		return fmt.Errorf("responder menu session does not match incident")
	}
	if state.SessionID != "" {
		l.sessionID = state.SessionID
	}
	l.pendingWrites = state.PendingWrites
	l.completedRepairs = state.CompletedRepairs
	l.continuation = state.Continuation
	l.menuSession = state.MenuSession
	l.menuReports = state.MenuReports
	l.menuRepairs = state.MenuRepairs
	l.menuExecutions = state.MenuExecutions
	l.batches = state.Batches
	if state.MaxBatches > 0 && state.MaxBatches < l.MaxBatches {
		l.MaxBatches = state.MaxBatches
	}
	l.failureSignals = state.FailureSignals
	return nil
}
