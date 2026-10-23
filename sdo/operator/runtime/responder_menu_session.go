package runtime

import (
	"context"
	"errors"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func (l *ResponderLoop) openMenuSession(ctx context.Context, response *ToolResponse) {
	if l.selection == nil {
		journal, err := openSelectionJournal(ctx, l.selectionStore, l.Request, l.sessionID, l.MaxBatches)
		if err != nil {
			response.Error = "durable selection session unavailable"
			return
		}
		if l.batches > 0 {
			journal.mu.Lock()
			next := journal.next()
			next.SelectionLedger.UsedBatches = uint32(l.batches)
			err = journal.save(ctx, next)
			journal.mu.Unlock()
			if err != nil {
				response.Error = "selection budget migration failed"
				return
			}
		}
		l.selection = journal
		l.mutationOwner = newMutationOwner(journal)
	}
	s := l.selection.snapshot()
	response.Session = &contracts.MitigationSession{Version: 1, IncidentId: s.IncidentId, SessionId: s.SessionId, DispatchEpoch: s.DispatchEpoch, OriginalDeadline: s.OriginalDeadline, Provider: s.Provider, ProviderSessionId: s.ProviderSessionId, CheckpointDigest: s.CheckpointDigest, PendingReports: l.selection.pendingReports(), WritesFrozen: s.WritesFrozen}
	response.Summary = "durable responder session"
}
func (l *ResponderLoop) checkpointMenuSession(ctx context.Context, checkpoint *contracts.MitigationSessionCheckpoint, response *ToolResponse) {
	if l.selection == nil || checkpoint == nil || wire.Validate(checkpoint) != nil {
		response.Error = "invalid responder session checkpoint"
		return
	}
	s := l.selection.snapshot()
	if checkpoint.IncidentId != s.IncidentId || checkpoint.SessionId != s.SessionId || checkpoint.DispatchEpoch != s.DispatchEpoch {
		response.Error = "stale responder session checkpoint"
		return
	}
	// A frozen replacement can redeliver/acknowledge an existing checkpoint and
	// reports, but cannot install a new identity or enable writes.
	if s.WritesFrozen {
		if checkpoint.Provider != s.Provider || checkpoint.ProviderSessionId != s.ProviderSessionId || checkpoint.CheckpointDigest != s.CheckpointDigest {
			response.Error = "frozen responder session cannot change checkpoint"
			return
		}
	} else if err := l.selection.checkpoint(ctx, checkpoint.Provider, checkpoint.ProviderSessionId, checkpoint.CheckpointDigest); err != nil {
		response.Error = "responder session checkpoint refused"
		return
	}
	for _, id := range checkpoint.AcknowledgedAttemptIds {
		if err := l.selection.acknowledge(ctx, id, checkpoint.ProviderSessionId, false); err != nil {
			response.Error = "responder report acknowledgment refused"
			return
		}
	}
	l.openMenuSession(ctx, response)
}
func (j *selectionJournal) persistLegacy(ctx context.Context, payload []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.frozen {
		return errSelectionFenced
	}
	next := j.next()
	next.ResponderToolStateJson = append([]byte(nil), payload...)
	return j.save(ctx, next)
}
func (l *ResponderLoop) attachSelectionJournal(ctx context.Context, request IncidentRequest, store StateStore, state *contracts.ControllerState, quiescent bool) error {
	l.selectionStore = store
	if state.GetSelectionLedger() == nil {
		return nil
	}
	journal, err := openSelectionJournal(ctx, store, request, l.sessionID, l.MaxBatches)
	if err != nil {
		return err
	}
	if err := journal.adopt(ctx, request.SourceCommit, quiescent); err != nil {
		return err
	}
	// Until the direct observation component is wired, missing fresh evidence is
	// an explicit failure. It never authorizes closure or automatic replay.
	if err := journal.recover(ctx, nil); err != nil {
		return err
	}
	s := journal.snapshot()
	l.selection = journal
	l.mutationOwner = newMutationOwner(journal)
	l.sessionID = s.SessionId
	l.Request.ResponseDeadline = s.OriginalDeadline.AsTime()
	l.MaxBatches = int(s.TotalBatches)
	l.batches = int(s.UsedBatches + s.ReservedBatches)
	return nil
}

// Only menu session participants use this gate. Existing capture-only sessions
// retain their evidence-gap semantics until they open a durable menu session.
func (l *ResponderLoop) selectionWriteGuard(ctx context.Context) error {
	if l.selection == nil {
		return nil
	}
	s := l.selection.snapshot()
	if err := l.selection.authorizeMutation(ctx, s.DispatchEpoch); err != nil {
		return errors.Join(errors.New("selection session prohibits mutations"), err)
	}
	return nil
}

func cloneSelectionReport(report *contracts.MitigationReport) *contracts.MitigationReport {
	if report == nil {
		return nil
	}
	return proto.Clone(report).(*contracts.MitigationReport)
}

func (l *ResponderLoop) withSelectionOwner(ctx context.Context, response *ToolResponse, call func(context.Context)) {
	if l.selection == nil {
		call(ctx)
		return
	}
	epoch := l.selection.snapshot().DispatchEpoch
	if err := l.mutationOwner.run(ctx, epoch, func(callCtx context.Context) error { call(callCtx); return nil }); err != nil {
		response.Error = "selection session prohibits mutations"
	}
}
func (l *ResponderLoop) selectionReport(_ context.Context, query *contracts.MitigationReportQuery, response *ToolResponse) {
	if l.selection == nil || query == nil || wire.Validate(query) != nil {
		response.Error = "invalid selection report query"
		return
	}
	s := l.selection.snapshot()
	if query.IncidentId != s.IncidentId || query.SessionId != s.SessionId || query.DispatchEpoch != s.DispatchEpoch {
		response.Error = "stale selection report query"
		return
	}
	a, err := selectionAttempt(s, query.AttemptId)
	if err != nil {
		response.Error = "unknown selection attempt"
		return
	}
	if a.Report == nil {
		response.Summary = "selection pending"
		return
	}
	response.Report = cloneSelectionReport(a.Report)
	response.Summary = "immutable selection report"
}
