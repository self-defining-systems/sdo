package runtime

import (
	"context"
	"encoding/json"
	"fmt"
)

// responderToolState retains limits and revision-bound outcomes, never prepared
// plans or mutation/rollback records. Version 1 journals are read only for these
// facts; their application fields are deliberately ignored.
type responderToolState struct {
	Version        int
	IncidentID     string
	SourceCommit   string
	FailureSignals []string
	Batches        int
	MaxBatches     int
}

func (l *ResponderLoop) persistToolState(ctx context.Context) error {
	if l.saveToolState == nil {
		return nil
	}
	payload, err := json.Marshal(responderToolState{Version: 2, IncidentID: l.Request.IncidentID, SourceCommit: l.Request.SourceCommit, FailureSignals: l.failureSignals, Batches: l.batches, MaxBatches: l.MaxBatches})
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
	if state.Batches < 0 || state.MaxBatches < 0 {
		return fmt.Errorf("invalid responder attempt budget")
	}
	l.batches = state.Batches
	if state.MaxBatches > 0 && state.MaxBatches < l.MaxBatches {
		l.MaxBatches = state.MaxBatches
	}
	l.failureSignals = state.FailureSignals
	return nil
}
