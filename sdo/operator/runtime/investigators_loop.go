package runtime

import (
	"context"
	"maps"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// HandleInvestigatorRead answers one read of an investigator: the same Go read as the responder's, on a private
// copy of the loop, so it records no tool call, takes no note queued for the responder, announces nothing and
// moves none of the responder's state.
func (l *ResponderLoop) HandleInvestigatorRead(ctx context.Context, request ToolRequest) ToolResponse {
	switch request.Command {
	case "detect", "changes", "timeline", "map", "source":
	default:
		return ToolResponse{Command: request.Command, Error: "investigators may only read: sdo detect, changes, timeline, map, source", Summary: "not allowed for an investigator"}
	}
	l.operationMu.RLock()
	defer l.operationMu.RUnlock()
	var read *ResponderLoop
	l.locked(func() {
		l.startNotes()
		read = &ResponderLoop{
			ExternalClues: l.ExternalClues, Request: l.liveRequest(), Programs: l.Programs, Provider: l.Provider, Detectors: l.Detectors,
			Executor: l.Executor, Traffic: l.Traffic, ReadView: l.ReadView, DiagnosisDeadline: l.DiagnosisDeadline,
			Now: l.Now, GitRead: l.GitRead, Trust: l.Trust, MenuConfig: l.MenuConfig,
			IncidentStatus: l.IncidentStatus, CurrentEvidence: l.CurrentEvidence, MutationGate: l.MutationGate, IncidentSources: l.IncidentSources,
			ExternalReceipts: l.ExternalReceipts, applicationsLog: append([]RecordedPlanApplication(nil), l.applicationsLog...),
			CorpusSessionRoot: l.CorpusSessionRoot, sessionID: l.sessionID, MaxBatches: l.MaxBatches,
			batches: l.batches, loopOpen: l.loopOpen, applySeq: l.applySeq, continuation: l.continuation,
			findings: append([]sdk.Finding(nil), l.findings...), preApplyCaptured: l.preApplyCaptured,
		}
		read.sourceCompared = maps.Clone(l.sourceCompared)
	})
	request.isolatedRead = true
	return read.Handle(ctx, request)
}

// investigatorBriefs renders the briefs of the top candidate nodes from a fresh read of the incident.
func (l *ResponderLoop) investigatorBriefs(ctx context.Context, max int) []investigatorBrief {
	if l.Provider == nil {
		return nil
	}
	snapshot, err := l.Provider.Snapshot(ctx)
	if err != nil {
		return nil
	}
	readView := program.ReadFrom(snapshot)
	if l.ReadView != nil {
		readView = l.ReadView(snapshot)
	}
	reads, ok := any(NewCycleReadView(readView)).(detectReads)
	if !ok {
		return nil
	}
	var request IncidentRequest
	l.locked(func() { request = l.liveRequest() })
	now := l.clock().UTC()
	input := detectInput{Request: request, Findings: request.Findings, View: reads, Now: now, AppState: l.pageInput(reads, request.Findings, now).AppState}
	set := buildSuspectSet(input)
	if set.onset.IsZero() {
		set.onset = now
	}
	view, _ := candidateViewFor(input, set)
	return investigatorBriefs(view, set, request, max)
}

// hasKnownWrite reports whether the responder made a write Go knows of (a plan application or, while the
// recording exists, a recorded write). With the recording off only SDO-mediated writes are visible.
func (l *ResponderLoop) hasKnownWrite() bool {
	var known bool
	l.locked(func() {
		times, _ := l.writeTimes()
		known = len(times) > 0
	})
	return known
}

// hasCandidateView reports whether the incident can show the candidate view the briefs come from: every
// incident with detectors can, and an external incident only with response.external_clues (otherwise
// `sdo detect` is disabled for it and no brief can be rendered).
func (l *ResponderLoop) hasCandidateView(request IncidentRequest) bool {
	return l != nil && (request.ExternalMessage == "" || l.ExternalClues)
}
