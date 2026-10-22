package runtime

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// recheckFastPath is read-only. Bound diagnoses are observations, never plans
// to replay. A NoMatch for every original binding and fresh independent health
// verification are both required before the controller may close the incident.
func (l *ResponderLoop) recheckFastPath(ctx context.Context, runs []ProgramRun) FastPathAttempt {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	attempt := FastPathAttempt{Findings: l.Request.Findings, Summary: "no bound prior diagnosis"}
	if len(runs) == 0 || l.Programs == nil {
		return attempt
	}
	selected := make([]selectedPlan, 0, len(runs))
	for _, run := range runs {
		p, ok := l.Programs.Diagnoser(run.ProgramID)
		if !ok {
			attempt.Summary = "program unavailable"
			return attempt
		}
		// The accepted request revision binds this dispatch and its observations.
		current := []ProgramRun{{ProgramID: run.ProgramID}}
		fillProgramRevisions(ctx, current, l.Request.RepositoryWorktree, l.Request.SourceCommit)
		if run.ProgramRevision == "" || current[0].ProgramRevision != run.ProgramRevision {
			attempt.Summary = "program revision changed"
			return attempt
		}
		if err := program.ValidateBindings(p.Spec(), run.Bindings); err != nil {
			attempt.Summary = err.Error()
			return attempt
		}
		selected = append(selected, selectedPlan{programID: run.ProgramID, bindings: run.Bindings})
		attempt.Programs = append(attempt.Programs, run.ProgramID)
	}
	response := ToolResponse{}
	l.verify(ctx, selected, &response)
	attempt.Verification = response
	if !response.Healthy {
		attempt.Summary = "independent verification did not clear"
		return attempt
	}
	if !boundPlansNoMatch(selected, response.ProgramRuns) || !fastPathFindingsVerified(attempt.Findings, response.DetectorStates) {
		attempt.Summary = "missing bound NoMatch or detector verification"
		return attempt
	}
	if l.Executor.Verifier == nil {
		attempt.Summary = "health verifier unavailable"
		return attempt
	}
	passed, err := l.Executor.Verifier.Verify(ctx)
	if err != nil || !passed {
		attempt.Summary = "independent health verification failed"
		return attempt
	}
	fillProgramRevisions(ctx, response.ProgramRuns, l.Request.RepositoryWorktree, l.Request.SourceCommit)
	attempt.Verification = response
	attempt.Resolved = true
	attempt.Summary = "live program diagnoses NoMatch and independent health verification passed after restart"
	return attempt
}
