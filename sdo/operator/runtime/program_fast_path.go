package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type FastPathAttempt struct {
	Attempted    bool
	Resolved     bool
	Programs     []string
	Summary      string
	RolledBack   bool
	RollbackErr  string
	Findings     []sdk.Finding
	Verification ToolResponse
}

// TryTrustedFastPath makes one settled, verified plan attempt for an incident.
// The caller hands an unsuccessful attempt to the responder with Summary.
func (l *ResponderLoop) TryTrustedFastPath(ctx context.Context, minSuccesses int) FastPathAttempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Request.RepairPolicy != "recorded-actions" || l.Programs == nil || !l.CorpusReplayPassed || l.batches > 0 {
		return FastPathAttempt{}
	}
	delay := l.DiagnosisSettleDelay
	if delay <= 0 {
		delay = DefaultDiagnosisSettleDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return FastPathAttempt{}
	case <-timer.C:
	}
	l.capturePhase = "open@settle"
	findings, runs, refreshError := l.refresh(ctx)
	l.capturePhase = ""
	if refreshError != "" {
		return FastPathAttempt{}
	}
	for _, finding := range l.Request.Findings {
		if finding.DetectorID == "drift/investigator" {
			return FastPathAttempt{}
		}
		if !strings.HasPrefix(finding.DetectorID, "drift/") || finding.Status != sdk.FindingActive {
			continue
		}
		findings = append(findings, finding)
	}
	if len(findings) == 0 {
		return FastPathAttempt{}
	}
	driftFindings := make([]sdk.Finding, 0)
	for _, finding := range findings {
		if strings.HasPrefix(finding.DetectorID, "drift/") {
			driftFindings = append(driftFindings, finding)
		}
	}
	if len(driftFindings) > 0 {
		view, err := l.snapshotView(ctx)
		if err != nil {
			return FastPathAttempt{}
		}
		driftRuns := SelectProgramCandidatesWithOptions(ctx, l.Programs, view, driftFindings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
		l.runs = append(l.runs, driftRuns...)
		runs = append(runs, driftRuns...)
	}
	l.findings = findings
	fillProgramRevisions(ctx, l.runs, l.Request.RepositoryWorktree, l.Request.SourceCommit)
	runs = l.runs[len(l.runs)-len(runs):]
	matches := make(map[string][]ProgramRun)
	for _, run := range runs {
		if run.Verdict.Result == program.NeedsJudgement {
			return FastPathAttempt{}
		}
		if run.Verdict.Result == program.Match && run.FindingFingerprint != "" {
			matches[run.FindingFingerprint] = append(matches[run.FindingFingerprint], run)
		}
	}
	selected := make([]ProgramRun, 0, len(findings))
	seen := map[string]bool{}
	for _, finding := range findings {
		covered := matches[FindingFingerprint(finding)]
		if len(covered) != 1 {
			return FastPathAttempt{}
		}
		run := covered[0]
		trust := l.Trust[run.ProgramID]
		if !trust.Trusted || trust.Failures != 0 || trust.VerifiedSuccesses < minSuccesses || run.ProgramRevision == "" || trust.LastChange != run.ProgramRevision {
			return FastPathAttempt{}
		}
		key := run.ProgramID + "\x00" + bindingKey(run.Bindings)
		if !seen[key] {
			selected = append(selected, run)
			seen[key] = true
		}
	}
	if len(selected) == 0 {
		return FastPathAttempt{}
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return FastPathAttempt{}
	}
	batch := program.Plan{}
	selectedPlans := make([]selectedPlan, 0, len(selected))
	attempt := FastPathAttempt{Programs: make([]string, 0, len(selected)), Findings: append([]sdk.Finding(nil), findings...)}
	for _, run := range selected {
		mitigator, ok := l.Programs.Mitigator(run.ProgramID)
		if !ok {
			return FastPathAttempt{}
		}
		plan, err := mitigator.Mitigate(ctx, view, run.Bindings)
		if err != nil || plan.Validate() != nil || len(plan.Actions) == 0 {
			return FastPathAttempt{}
		}
		selectedPlans = append(selectedPlans, selectedPlan{programID: run.ProgramID, bindings: run.Bindings, plan: plan})
		batch.Actions = append(batch.Actions, plan.Actions...)
		attempt.Programs = append(attempt.Programs, run.ProgramID)
	}
	sort.Strings(attempt.Programs)
	l.fastPathPrograms = append([]string(nil), attempt.Programs...)
	started := time.Now().UTC()
	l.batches++
	attempt.Attempted = true
	preview, prepareErr := l.Executor.Apply(ctx, l.Request, batch, true)
	if prepareErr == nil && l.BeforeFastPathApply != nil {
		prepareErr = l.BeforeFastPathApply(ctx, attempt.Programs, preview.PreImages)
	}
	if prepareErr != nil {
		attempt.Summary = fmt.Sprintf("trusted programs %s attempted; plan preparation failed: %v", strings.Join(attempt.Programs, ", "), prepareErr)
		for _, id := range attempt.Programs {
			l.addFailureSignal("fast_path_failure:" + id)
		}
		l.applicationsLog = append(l.applicationsLog, RecordedPlanApplication{ID: "fast-path-1", ProgramIDs: attempt.Programs, Result: PlanApplicationResult{Plan: batch}, Error: prepareErr.Error(), StartedAt: started, CompletedAt: time.Now().UTC()})
		return attempt
	}
	result, applyErr := l.Executor.Apply(ctx, l.Request, batch, false)
	response := ToolResponse{}
	if result.Applied {
		l.verify(ctx, selectedPlans, &response)
	}
	attempt.Verification = response
	attempt.Resolved = applyErr == nil && result.VerificationPassed && response.Healthy
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
			if rollbackErr := l.Executor.Rollback(ctx, result); rollbackErr != nil {
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
	l.applicationsLog = append(l.applicationsLog, entry)
	l.calls = append(l.calls, ToolCall{Command: "fast-path", At: time.Now().UTC(), Summary: boundedToolText(attempt.Summary)})
	return attempt
}
