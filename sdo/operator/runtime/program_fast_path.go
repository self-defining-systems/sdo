package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type FastPathAttempt struct {
	Attempted               bool
	SkippedReason           FastPathSkipReason
	Resolved                bool
	Programs                []string
	Summary                 string
	RolledBack              bool
	RollbackErr             string
	Findings                []sdk.Finding
	Verification            ToolResponse
	RelationCoveredFindings []RelationCoveredFinding
}

type RelationCoveredFinding struct {
	FindingFingerprint string           `json:"finding_fingerprint"`
	ProgramID          string           `json:"program_id"`
	Bindings           program.Bindings `json:"bindings"`
}

type FastPathSkipReason string

const (
	FastPathSkipRepairPolicy        FastPathSkipReason = "repair_policy"
	FastPathSkipProgramsUnavailable FastPathSkipReason = "programs_unavailable"
	FastPathSkipCorpusReplay        FastPathSkipReason = "corpus_replay_unavailable"
	FastPathSkipResponseStarted     FastPathSkipReason = "response_started"
	FastPathSkipSettleCancelled     FastPathSkipReason = "settle_cancelled"
	FastPathSkipRefreshFailed       FastPathSkipReason = "settle_refresh_failed"
	FastPathSkipUnexplainedDrift    FastPathSkipReason = "unexplained_drift"
	FastPathSkipNoFindings          FastPathSkipReason = "no_active_findings"
	FastPathSkipSnapshotFailed      FastPathSkipReason = "snapshot_failed"
	FastPathSkipNeedsJudgement      FastPathSkipReason = "needs_judgement"
	FastPathSkipUncoveredFinding    FastPathSkipReason = "uncovered_finding"
	FastPathSkipAmbiguousMatch      FastPathSkipReason = "ambiguous_match"
	FastPathSkipUntrustedProgram    FastPathSkipReason = "untrusted_program"
	FastPathSkipNoSelectedProgram   FastPathSkipReason = "no_selected_program"
	FastPathSkipMitigatorMissing    FastPathSkipReason = "mitigator_missing"
	FastPathSkipInvalidPlan         FastPathSkipReason = "invalid_plan"
)

func selectTrustedFastPathRuns(findings []sdk.Finding, runs []ProgramRun, trust map[string]ProgramTrust, minSuccesses int, view sdk.DetectionContext) ([]ProgramRun, []RelationCoveredFinding, FastPathSkipReason) {
	matches := make(map[string]map[string]ProgramRun)
	for _, run := range runs {
		if run.Verdict.Result == program.NeedsJudgement {
			return nil, nil, FastPathSkipNeedsJudgement
		}
		if run.Verdict.Result != program.Match || run.FindingFingerprint == "" {
			continue
		}
		if matches[run.FindingFingerprint] == nil {
			matches[run.FindingFingerprint] = map[string]ProgramRun{}
		}
		key := run.ProgramID + "\x00" + bindingKey(run.Bindings)
		matches[run.FindingFingerprint][key] = run
	}
	selected := make([]ProgramRun, 0, len(findings))
	seen := map[string]bool{}
	uncovered := make([]sdk.Finding, 0)
	for _, finding := range findings {
		covered := matches[FindingFingerprint(finding)]
		if len(covered) == 0 {
			uncovered = append(uncovered, finding)
			continue
		}
		if len(covered) != 1 {
			return nil, nil, FastPathSkipAmbiguousMatch
		}
		for key, run := range covered {
			state := trust[run.ProgramID]
			if !state.Trusted || state.Failures != 0 || state.VerifiedSuccesses < minSuccesses || run.ProgramRevision == "" || state.LastChange != run.ProgramRevision {
				return nil, nil, FastPathSkipUntrustedProgram
			}
			if !seen[key] {
				selected = append(selected, run)
				seen[key] = true
			}
		}
	}
	if len(selected) == 0 {
		if len(uncovered) > 0 {
			return nil, nil, FastPathSkipUncoveredFinding
		}
		return nil, nil, FastPathSkipNoSelectedProgram
	}
	related := make([]RelationCoveredFinding, 0, len(uncovered))
	for _, finding := range uncovered {
		matched := false
		for _, run := range selected {
			for _, bound := range run.Bindings {
				if !fastPathDirectlyRelated(view, finding.PrimaryResource, bound) {
					continue
				}
				related = append(related, RelationCoveredFinding{FindingFingerprint: FindingFingerprint(finding), ProgramID: run.ProgramID, Bindings: run.Bindings})
				matched = true
				break
			}
			if matched {
				break
			}
		}
		if !matched {
			return nil, nil, FastPathSkipUncoveredFinding
		}
	}
	return selected, related, ""
}

// TryTrustedFastPath makes one settled, verified plan attempt for an incident.
// The caller hands an unsuccessful attempt to the responder with Summary.
func (l *ResponderLoop) TryTrustedFastPath(ctx context.Context, minSuccesses int) FastPathAttempt {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	skip := func(reason FastPathSkipReason) FastPathAttempt {
		l.fastPathSkippedReason = reason
		return FastPathAttempt{SkippedReason: reason}
	}
	if l.Request.RepairPolicy != "recorded-actions" {
		return skip(FastPathSkipRepairPolicy)
	}
	if l.Programs == nil {
		return skip(FastPathSkipProgramsUnavailable)
	}
	if !l.CorpusReplayPassed {
		return skip(FastPathSkipCorpusReplay)
	}
	if l.batches > 0 {
		return skip(FastPathSkipResponseStarted)
	}
	// Only immutable capability and trust can rule out a future settled match.
	// An initial NoMatch may be transient while the workload is rolling out.
	reason := FastPathSkipNoSelectedProgram
	for _, spec := range l.Programs.List() {
		if _, ok := l.Programs.Mitigator(spec.ID); !ok {
			if reason == FastPathSkipNoSelectedProgram {
				reason = FastPathSkipMitigatorMissing
			}
			continue
		}
		reason = FastPathSkipUntrustedProgram
		state := l.Trust[spec.ID]
		if !state.Trusted || state.Failures != 0 || state.VerifiedSuccesses < minSuccesses {
			continue
		}
		runs := []ProgramRun{{ProgramID: spec.ID}}
		fillProgramRevisions(ctx, runs, l.Request.RepositoryWorktree, l.Request.SourceCommit)
		if runs[0].ProgramRevision != "" && state.LastChange == runs[0].ProgramRevision {
			reason = ""
			break
		}
	}
	if reason != "" {
		return skip(reason)
	}
	delay := l.DiagnosisSettleDelay
	if delay <= 0 {
		delay = DefaultDiagnosisSettleDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return skip(FastPathSkipSettleCancelled)
	case <-timer.C:
	}
	findings, runs, refreshError := l.refresh(ctx)
	if refreshError != "" {
		return skip(FastPathSkipRefreshFailed)
	}
	for _, finding := range findings {
		if finding.DetectorID == "drift.unexplained" {
			return skip(FastPathSkipUnexplainedDrift)
		}
	}
	if len(findings) == 0 {
		return skip(FastPathSkipNoFindings)
	}
	driftFindings := make([]sdk.Finding, 0)
	for _, finding := range findings {
		if strings.HasPrefix(finding.DetectorID, "drift.") {
			driftFindings = append(driftFindings, finding)
		}
	}
	if len(driftFindings) > 0 {
		view := l.lastDiagnosisView
		if view == nil {
			return skip(FastPathSkipSnapshotFailed)
		}
		driftRuns := SelectProgramCandidatesWithOptions(ctx, l.Programs, view, driftFindings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
		l.rememberMatchedViews(ctx, driftRuns, view)
		l.runs = append(l.runs, driftRuns...)
		runs = append(runs, driftRuns...)
	}
	l.findings = findings
	fillProgramRevisions(ctx, l.runs, l.Request.RepositoryWorktree, l.Request.SourceCommit)
	runs = l.runs[len(l.runs)-len(runs):]
	view := l.lastDiagnosisView
	if view == nil {
		return skip(FastPathSkipSnapshotFailed)
	}
	selected, related, reason := selectTrustedFastPathRuns(findings, runs, l.Trust, minSuccesses, view)
	if reason != "" {
		return skip(reason)
	}
	batch := program.Plan{}
	selectedPlans := make([]selectedPlan, 0, len(selected))
	attempt := FastPathAttempt{Programs: make([]string, 0, len(selected)), Findings: append([]sdk.Finding(nil), findings...), RelationCoveredFindings: related}
	l.fastPathRelationCoveredFindings = append([]RelationCoveredFinding(nil), related...)
	for _, run := range selected {
		_, ok := l.Programs.Mitigator(run.ProgramID)
		if !ok {
			return skip(FastPathSkipMitigatorMissing)
		}
		matchedView := l.matchedViews[matchedViewKey(run.ProgramID, run.Bindings)]
		if matchedView == nil {
			return skip(FastPathSkipSnapshotFailed)
		}
		key := matchedViewKey(run.ProgramID, run.Bindings)
		preview, cached := l.matchedPlans[key]
		plan := preview.Plan
		if err := l.matchedPlanErrors[key]; err != nil {
			refused := skip(FastPathSkipInvalidPlan)
			refused.Summary = fmt.Sprintf("trusted program %s plan refused: %v", run.ProgramID, err)
			return refused
		}
		if !cached || plan.Validate() != nil || len(plan.Actions) == 0 {
			return skip(FastPathSkipInvalidPlan)
		}
		selectedPlans = append(selectedPlans, selectedPlan{programID: run.ProgramID, bindings: run.Bindings, plan: plan})
		batch.Actions = append(batch.Actions, plan.Actions...)
		attempt.Programs = append(attempt.Programs, run.ProgramID)
	}
	sort.Strings(attempt.Programs)
	l.fastPathPrograms = append([]string(nil), attempt.Programs...)
	started := time.Now().UTC()
	planIDs := make([]string, 0, len(selectedPlans))
	for i, plan := range selectedPlans {
		id := fmt.Sprintf("fast-path-plan-%d", i+1)
		planIDs = append(planIDs, id)
		requestJSON, _ := json.Marshal(ToolRequest{Command: "plan", ProgramID: plan.programID, Bindings: plan.bindings})
		responseJSON, _ := json.Marshal(ToolResponse{Command: "plan", PlanID: id})
		l.calls = append(l.calls, ToolCall{Command: "plan", At: started, RequestJSON: string(requestJSON), ResponseJSON: string(responseJSON), Summary: "trusted program prepared plan"})
	}
	l.batches++
	attempt.Attempted = true
	l.applicationsLog = append(l.applicationsLog, RecordedPlanApplication{ID: "fast-path-1", ProgramIDs: attempt.Programs, Result: PlanApplicationResult{Plan: batch}, StartedAt: started, Pending: true})
	if err := l.persistToolState(ctx); err != nil {
		attempt.Summary = err.Error()
		return attempt
	}
	_, prepareErr := l.Executor.Apply(ctx, l.Request, batch, true)
	if prepareErr == nil && l.BeforeFastPathApply != nil {
		prepareErr = l.BeforeFastPathApply(ctx, selected)
	}
	if prepareErr == nil {
		// Capture failure is recorded separately from plan preparation failure.
		_ = l.capturePreApply(ctx)
	}
	if prepareErr != nil {
		attempt.Summary = fmt.Sprintf("trusted programs %s attempted; plan preparation failed: %v", strings.Join(attempt.Programs, ", "), prepareErr)
		for _, id := range attempt.Programs {
			l.addFailureSignal("fast_path_failure:" + id)
		}
		l.applicationsLog[len(l.applicationsLog)-1] = RecordedPlanApplication{ID: "fast-path-1", ProgramIDs: attempt.Programs, Result: PlanApplicationResult{Plan: batch}, Error: prepareErr.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}
		return attempt
	}
	executor := l.Executor
	result, applyErr := executor.Apply(ctx, l.Request, batch, false)
	response := ToolResponse{}
	if result.Applied {
		l.verify(ctx, selectedPlans, &response)
	}
	fillProgramRevisions(ctx, response.ProgramRuns, l.Request.RepositoryWorktree, l.Request.SourceCommit)
	attempt.Verification = response
	attempt.Resolved = applyErr == nil && result.VerificationPassed && response.Healthy
	if attempt.Resolved {
		attempt.Resolved = boundPlansNoMatch(selectedPlans, response.ProgramRuns) && fastPathFindingsVerified(attempt.Findings, response.DetectorStates)
	}
	if attempt.Resolved {
		for _, run := range response.ProgramRuns {
			if run.Verdict.Result != program.NoMatch {
				attempt.Resolved = false
				break
			}
		}
	}
	if attempt.Resolved {
		attempt.Summary = fmt.Sprintf("trusted programs %s resolved incident", strings.Join(attempt.Programs, ", "))
	} else {
		result.VerificationPassed = false
		for _, id := range attempt.Programs {
			l.addFailureSignal("fast_path_failure:" + id)
		}
		if len(result.ActionsApplied) > 0 {
			if rollbackErr := executor.rollback(ctx, &result); rollbackErr != nil {
				attempt.RollbackErr = rollbackErr.Error()
			} else {
				attempt.RolledBack = true
			}
		}
		attempt.Summary = fmt.Sprintf("trusted programs %s attempted; verification failed; rollback %t", strings.Join(attempt.Programs, ", "), attempt.RolledBack)
		if applyErr != nil {
			attempt.Summary += ": " + applyErr.Error()
		}
		if attempt.RollbackErr != "" {
			attempt.Summary += ": rollback failed: " + attempt.RollbackErr
		}
	}
	entry := RecordedPlanApplication{ID: "fast-path-1", ProgramIDs: attempt.Programs, Result: result, StartedAt: started, CompletedAt: time.Now().UTC(), RolledBack: attempt.RolledBack, RollbackError: attempt.RollbackErr}
	if applyErr != nil {
		entry.Error = applyErr.Error()
	}
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == entry.ID {
			l.applicationsLog[i] = entry
		}
	}
	if attempt.RolledBack {
		result.RolledBack = true
		result.RollingBack = false
		result.RollbackCompleted = len(result.ActionsApplied)
	}
	if attempt.RollbackErr != "" {
		result.RollbackError = attempt.RollbackErr
		result.RollingBack = true
	}
	l.applications["fast-path-1"] = result
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == entry.ID {
			l.applicationsLog[i].Result = result
		}
	}
	response.Command = "apply"
	response.ApplicationID = "fast-path-1"
	if applyErr != nil {
		response.Error = applyErr.Error()
	}
	if !attempt.Resolved && response.Error == "" {
		response.Error = "trusted fast-path verification failed"
	}
	requestJSON, _ := json.Marshal(ToolRequest{Command: "apply", PlanIDs: planIDs})
	responseJSON, _ := json.Marshal(response)
	l.calls = append(l.calls, ToolCall{Command: "apply", At: time.Now().UTC(), RequestJSON: string(requestJSON), ResponseJSON: string(responseJSON), Error: response.Error, Summary: attempt.Summary})
	l.calls = append(l.calls, ToolCall{Command: "fast-path", At: time.Now().UTC(), Summary: boundedToolText(attempt.Summary)})
	return attempt
}

func fastPathFindingsVerified(findings []sdk.Finding, states []DetectorEvaluation) bool {
	verified := map[string]bool{}
	for _, state := range states {
		if state.Status == DetectorEvaluationClear {
			verified[state.DetectorID] = true
		}
	}
	for _, finding := range findings {
		if !strings.HasPrefix(finding.DetectorID, "drift.") && !verified[finding.DetectorID] {
			return false
		}
	}
	return true
}
