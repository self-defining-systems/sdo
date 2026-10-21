package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type persistedSelectedPlan struct {
	ID        string
	ProgramID string
	Bindings  program.Bindings
	Result    PlanApplicationResult
}

// responderToolState is controller-owned, incident-scoped state. Action types
// use the executor's existing versioned JSON edge rather than interface maps.
type responderToolState struct {
	Version                         int
	IncidentID                      string
	SourceCommit                    string
	Plans                           map[string]persistedSelectedPlan
	Applications                    map[string]PlanApplicationResult
	ApplicationsLog                 []RecordedPlanApplication
	FailureSignals                  []string
	Calls                           []ToolCall
	Runs                            []ProgramRun
	Findings                        []sdk.Finding
	Batches                         int
	FastPathPrograms                []string
	FastPathSkippedReason           FastPathSkipReason
	FastPathRelationCoveredFindings []RelationCoveredFinding
}

func (l *ResponderLoop) toolState() responderToolState {
	plans := make(map[string]persistedSelectedPlan, len(l.plans))
	for id, p := range l.plans {
		plans[id] = persistedSelectedPlan{ID: id, ProgramID: p.programID, Bindings: p.bindings, Result: PlanApplicationResult{Plan: p.plan}}
	}
	return responderToolState{Version: 1, IncidentID: l.Request.IncidentID, SourceCommit: l.Request.SourceCommit, Plans: plans, Applications: l.applications, ApplicationsLog: l.applicationsLog, FailureSignals: l.failureSignals, Calls: l.calls, Runs: l.runs, Findings: l.findings, Batches: l.batches, FastPathPrograms: l.fastPathPrograms, FastPathSkippedReason: l.fastPathSkippedReason, FastPathRelationCoveredFindings: l.fastPathRelationCoveredFindings}
}
func (l *ResponderLoop) persistToolState(ctx context.Context) error {
	if l.saveToolState == nil {
		return nil
	}
	payload, err := json.Marshal(l.toolState())
	if err != nil {
		return fmt.Errorf("encode responder tool state: %w", err)
	}
	if err := l.saveToolState(ctx, payload); err != nil {
		return fmt.Errorf("persist responder tool state: %w", err)
	}
	return nil
}
func (l *ResponderLoop) restoreToolSnapshot(payload []byte) error {
	var state responderToolState
	if err := json.Unmarshal(payload, &state); err != nil {
		return fmt.Errorf("decode responder tool state: %w", err)
	}
	if state.Version != 1 || state.IncidentID != l.Request.IncidentID || state.SourceCommit != l.Request.SourceCommit {
		return fmt.Errorf("responder tool state does not match incident/revision")
	}
	for id, p := range state.Plans {
		if id != p.ID {
			return fmt.Errorf("invalid prepared plan identity")
		}
		if err := p.Result.Plan.Validate(); err != nil {
			return err
		}
		l.plans[id] = selectedPlan{id: id, programID: p.ProgramID, bindings: p.Bindings, plan: p.Result.Plan}
	}
	if state.Applications != nil {
		l.applications = state.Applications
	}
	l.applicationsLog = state.ApplicationsLog
	l.failureSignals = state.FailureSignals
	l.calls = state.Calls
	l.runs = state.Runs
	l.findings = state.Findings
	l.batches = state.Batches
	l.fastPathPrograms = state.FastPathPrograms
	l.fastPathSkippedReason = state.FastPathSkippedReason
	l.fastPathRelationCoveredFindings = state.FastPathRelationCoveredFindings
	return nil
}
func (l *ResponderLoop) applicationExecutor(id string) PlanExecutor {
	executor := l.Executor
	previousCheckpoint := executor.Checkpoint
	executor.Checkpoint = func(ctx context.Context, result PlanApplicationResult) error {
		l.applications[id] = result
		for i := range l.applicationsLog {
			if l.applicationsLog[i].ID == id {
				l.applicationsLog[i].Result = result
				l.applicationsLog[i].RolledBack = result.RolledBack
				l.applicationsLog[i].RollbackError = result.RollbackError
			}
		}
		if err := l.persistToolState(ctx); err != nil {
			return err
		}
		if previousCheckpoint != nil {
			return previousCheckpoint(ctx, result)
		}
		return nil
	}
	return executor
}

// reconcileApplications never replays a forward mutation. Interrupted attempts
// consume their original budget and retain both the handle and failure evidence.
func (l *ResponderLoop) reconcileApplications(ctx context.Context) error {
	for i := range l.applicationsLog {
		entry := l.applicationsLog[i]
		if !entry.Pending && !entry.Result.RollingBack && entry.Result.PendingAction == nil {
			continue
		}
		executor := l.applicationExecutor(entry.ID)
		result, err := executor.Reconcile(ctx, entry.Result)
		l.addFailureSignal("interrupted_application:" + entry.ID)
		if err == nil && len(result.ActionsApplied) > 0 && !result.RolledBack {
			err = executor.rollback(ctx, &result)
			if err == nil {
				result.RolledBack = true
				result.RollingBack = false
				result.RollbackCompleted = len(result.ActionsApplied)
			}
		}
		if err != nil {
			result.RollbackError = err.Error()
			l.addFailureSignal("rollback_failure:" + entry.ID)
		}
		l.applications[entry.ID] = result
		l.applicationsLog[i].Result = result
		l.applicationsLog[i].RolledBack = result.RolledBack
		l.applicationsLog[i].RollbackError = result.RollbackError
		l.applicationsLog[i].Pending = err != nil
		l.applicationsLog[i].Error = "application interrupted by controller restart"
		l.applicationsLog[i].CompletedAt = time.Now().UTC()
		// Conflicts remain addressable by rollback and are never overwritten.
		if err := l.persistToolState(ctx); err != nil {
			return err
		}
	}
	return nil
}
