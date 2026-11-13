package runtime

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"maps"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"

	"sdo.dev/operator/sdk"
)

func concurrentRead(command string) bool {
	switch command {
	case "status", "detect", "diagnose", "menu", "source":
		return true
	}
	return false
}

// Reads pin execution state against mutations, then evaluate with private
// presentation/recording state. Only preparation and publication take mu; slow
// API reads never serialize unrelated observations. Registries are immutable
// after loadWorkingPrograms publishes a version.
func (l *ResponderLoop) handleRead(ctx context.Context, request ToolRequest) (response ToolResponse) {
	l.operationMu.RLock()
	defer l.operationMu.RUnlock()
	var read *ResponderLoop
	var prepareErr error
	l.locked(func() {
		if request.Command == "menu" || request.Command == "diagnose" {
			prepareErr = l.loadWorkingPrograms(ctx, request.ProgramID)
		}
		read = &ResponderLoop{
			Request: l.Request, Programs: l.Programs, Provider: l.Provider, Detectors: l.Detectors,
			Executor: l.Executor, ReadView: l.ReadView, DiagnosisDeadline: l.DiagnosisDeadline,
			Now: l.Now, GitRead: l.GitRead, Trust: l.Trust, MenuConfig: l.MenuConfig,
			IncidentStatus: l.IncidentStatus, CurrentEvidence: l.CurrentEvidence, MutationGate: l.MutationGate, IncidentPages: l.IncidentPages,
			CorpusSessionRoot: l.CorpusSessionRoot, sessionID: l.sessionID, MaxBatches: l.MaxBatches,
			batches: l.batches, loopOpen: l.loopOpen, applySeq: l.applySeq, continuation: l.continuation,
			findings: append([]sdk.Finding(nil), l.findings...), preApplyCaptured: l.preApplyCaptured,
		}
		read.sourceCompared = maps.Clone(l.sourceCompared)
		if l.menuSession != nil {
			read.menuSession = proto.Clone(l.menuSession).(*contracts.MitigationSession)
		}
	})
	request.isolatedRead = true
	if prepareErr != nil {
		response = ToolResponse{Command: request.Command, Error: prepareErr.Error(), Summary: prepareErr.Error()}
	} else {
		response = read.Handle(ctx, request)
	}
	l.locked(func() {
		if prepareErr == nil {
			l.Request.Findings = read.Request.Findings
			l.Request.DriftEvents = read.Request.DriftEvents
		}
		if prepareErr == nil && (request.Command == "status" || request.Command == "diagnose" && request.ProgramID == "") {
			l.findings = read.findings
			l.drift = read.drift
		}
		l.runs = append(l.runs, read.runs...)
		if read.sourceCompared != nil {
			if l.sourceCompared == nil {
				l.sourceCompared = map[string]bool{}
			}
			maps.Copy(l.sourceCompared, read.sourceCompared)
		}
		if read.menu != nil {
			l.menu = read.menu
			l.menuSession = read.menuSession
			l.menuEntries.advertise(read.menuEntries.entries)
		}
		if notes := l.takeStageNotes(request); len(notes) > 0 {
			response.PriorStages = notes
			lines := make([]string, len(notes))
			for i, note := range notes {
				lines[i] = note.String()
			}
			response.Summary = "automatic stage ran while you worked: " + strings.Join(lines, "; ") + ". " + response.Summary
		}
		raw, _ := json.Marshal(request)
		l.calls = append(l.calls, ToolCall{Command: request.Command, At: l.clock().UTC(), RequestJSON: string(raw), ResponseJSON: string(recordedToolResponseJSON(response, nil)), Error: response.Error, Summary: response.Summary})
		if err := l.persistToolState(ctx); err != nil {
			response.Error = err.Error()
			response.Summary = response.Error
		}
	})
	return response
}
