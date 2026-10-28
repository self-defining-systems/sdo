package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"runtime/debug"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// DefaultStageBudget is the one owner of the automatic stage's time: the
// checks, the single apply with its rollout wait, and the post-repair
// observation share it. Matches the stage could not apply or verify inside it
// go to the agent in the stage report (skip reason "stage_budget").
const DefaultStageBudget = 45 * time.Second

const stagePlanNotExecutable = "plan_not_executable"

// DeterministicResponseStage uses the same plan/apply and immutable receipt
// boundary as the responder, operating only on accepted registry programs.
type DeterministicResponseStage struct {
	NewLoop func(IncidentRequest) *ResponderLoop
	Restore func(context.Context, IncidentRequest, *ResponderLoop) error
	// KeepLoop leaves the loop NewLoop returns open: it belongs to a running
	// responder, so a later pass must not close its working programs.
	KeepLoop bool
	// Budget is the stage's total time (DefaultStageBudget when unset).
	Budget time.Duration
	// RolloutTimeout and ObservationCeiling can only shorten what the budget
	// leaves for the rollout wait and the post-repair observation.
	RolloutTimeout      time.Duration
	ObservationInterval time.Duration
	ObservationCeiling  time.Duration
}

type responseStageProgress struct {
	Report             *contracts.StageReport
	Coverage           *contracts.PlaybookCoverage
	InitialProgramRuns []ProgramRun
	Complete           bool
}

// Restore decisions for automatic refresh and ordinary tool rejoin alike;
// unchanged accepted code cannot regain retries by crossing a checkpoint.
func (l *ResponderLoop) restoreTerminalStagePlanErrors() {
	for _, progress := range l.stageProgress {
		for _, attempt := range progress.Report.GetMitigations() {
			if attempt.SkipReason == stagePlanNotExecutable {
				l.rememberTerminalPlanError(attempt.PlaybookId, attempt.ProgramRevision, &NonExecutablePlanError{Cause: errors.New(attempt.Error)})
			}
		}
	}
}

func (*DeterministicResponseStage) Name() string { return "deterministic" }

func (stage *DeterministicResponseStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	started := time.Now()
	budget := stage.Budget
	if budget <= 0 {
		budget = DefaultStageBudget
	}
	remaining := func() time.Duration { return budget - time.Since(started) }
	report := &contracts.StageReport{Stage: stage.Name()}
	if stage.NewLoop == nil {
		return report, nil
	}
	loop := stage.NewLoop(state.Request)
	if loop == nil {
		return report, nil
	}
	if !stage.KeepLoop {
		defer loop.CloseWorkingPrograms()
	}
	defer func() {
		state.RepairActions = loop.ApplicationRepairReceipts(state.RepairActions)
		state.ProgramToolCalls = loop.ToolCalls()
	}()
	if stage.Restore != nil {
		if err := stage.Restore(ctx, state.Request, loop); err != nil {
			return report, err
		}
	}
	if loop.stageProgress == nil {
		loop.stageProgress = map[string]responseStageProgress{}
	}
	progress := loop.stageProgress[stage.Name()]
	for _, prior := range state.PriorStages {
		loop.restoreScriptRefusals(prior)
	}
	loop.restoreScriptRefusals(progress.Report)
	if progress.Report != nil {
		report = proto.Clone(progress.Report).(*contracts.StageReport)
		state.Coverage = cloneCoverage(progress.Coverage)
		state.Request.PlaybookCoverage = cloneCoverage(progress.Coverage)
		state.Request.InitialProgramRuns = append([]ProgramRun(nil), progress.InitialProgramRuns...)
		loop.Request = state.Request
	}
	loop.restoreTerminalStagePlanErrors()

	save := func(complete bool) error {
		retainAdmissionStageFindings(state, report)
		report.RemainingFindingFingerprints = findingFingerprints(state.Findings)
		loop.stageProgress[stage.Name()] = responseStageProgress{Report: report, Coverage: cloneCoverage(state.Coverage), InitialProgramRuns: append([]ProgramRun(nil), state.Request.InitialProgramRuns...), Complete: complete}
		return loop.persistToolState(ctx)
	}
	// Always reobserve after recovery. A recorded attempt is never replayed,
	// including one interrupted between the write and its completed receipt.
	if progress.Report != nil {
		func() {
			loop.operationMu.Lock()
			defer loop.operationMu.Unlock()
			loop.mu.Lock()
			defer loop.mu.Unlock()
			var selected []selectedPlan
			for _, attempt := range report.Mitigations {
				if attempt.GetResult() == "applied_cleared" && attempt.GetProgramRevision() == loop.programRevision(attempt.GetPlaybookId()) {
					selected = append(selected, selectedPlan{programID: attempt.PlaybookId, revision: attempt.ProgramRevision, bindings: programBindingsState(attempt.Bindings)})
				}
			}
			response := ToolResponse{}
			loop.verify(ctx, selected, &response, true)
			updateStageFindings(state, response)
			for _, attempt := range report.Mitigations {
				for _, run := range response.ProgramRuns {
					if attempt.GetResult() == "applied_cleared" && run.ProgramID == attempt.PlaybookId && run.ProgramRevision == attempt.ProgramRevision && run.Bindings.Equal(programBindingsState(attempt.Bindings)) {
						fresh := proto.Clone(attempt).(*contracts.StageMitigation)
						fresh.PostApplyDiagnose = programRunContract(run)
						clearVerifiedStageTargets(state, fresh, attempt.ClearedFindingFingerprints, response)
					}
				}
			}
		}()
		if progress.Complete && !state.Refresh {
			return report, save(true)
		}
	}
	// A later pass rebuilds coverage for what is still open, from a fresh
	// diagnosis. The opening capture is immutable evidence, so it is not retaken.
	if state.Refresh {
		state.Coverage = nil
	}
	if state.Coverage == nil {
		var runs []ProgramRun
		if state.Refresh {
			runs = loop.RefreshDiagnose(ctx)
			fillProgramRevisions(ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
			runs = append(append([]ProgramRun(nil), state.Request.InitialProgramRuns...), runs...)
		} else {
			runs = loop.InitialDiagnose(ctx)
			fillProgramRevisions(ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		}
		state.Request.InitialProgramRuns = runs
		state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, loop.Trust)
		state.Request.PlaybookCoverage = cloneCoverage(state.Coverage)
		loop.Request = state.Request
	}
	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	loop.mu.Lock()
	defer loop.mu.Unlock()
	matches := stageCandidates(state)
	attempted := map[string]bool{}
	// A later pass may complete a binding an earlier one only skipped, because
	// its finding had not activated yet. A skip wrote nothing, so that attempt
	// is redone in its place; any attempt that wrote is never repeated.
	retry := map[string]*contracts.StageMitigation{}
	for _, attempt := range report.Mitigations {
		key := stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))
		if state.Refresh && attempt.Result == "skipped" && attempt.SkipReason != stagePlanNotExecutable {
			retry[key] = attempt
			continue
		}
		attempted[key] = true
	}
	var ready []stageReady
	for _, match := range matches {
		if len(state.Findings) == 0 {
			break
		}
		if err := ctx.Err(); err != nil {
			return report, err
		}
		bindings := programBindingsState(match.Bindings)
		key := stageAttemptKey(match.PlaybookId, bindings)
		if attempted[key] {
			continue
		}
		attempted[key] = true
		attempt := &contracts.StageMitigation{PlaybookId: match.PlaybookId, ProgramRevision: match.ProgramRevision, Bindings: programBindingsContract(bindings), Result: "skipped"}
		if attempt.ProgramRevision == "" {
			attempt.ProgramRevision = loop.programRevision(match.PlaybookId)
		}
		if earlier := retry[key]; earlier != nil {
			proto.Reset(earlier)
			proto.Merge(earlier, attempt)
			attempt = earlier
		} else {
			report.Mitigations = append(report.Mitigations, attempt)
		}
		skip := func(reason string) error {
			attempt.SkipReason = reason
			recordStageFindingProgress(attempt, match.targets, state.Findings)
			return save(false)
		}
		if terminal := loop.terminalPlanError(match.PlaybookId); terminal != nil {
			attempt.Error = terminal.Error()
			if err := skip(stagePlanNotExecutable); err != nil {
				return report, err
			}
			continue
		}
		if remaining() <= 0 {
			if err := skip("stage_budget"); err != nil {
				return report, err
			}
			continue
		}
		if state.Request.RepairPolicy != "recorded-actions" {
			if err := skip("repair_policy"); err != nil {
				return report, err
			}
			continue
		}
		if loop.Programs == nil {
			if err := skip("no_mitigation"); err != nil {
				return report, err
			}
			continue
		}
		count := 0
		for _, spec := range loop.Programs.List() {
			if spec.ID == match.PlaybookId {
				count++
			}
		}
		if count > 1 {
			if err := skip("multiple_entry_points"); err != nil {
				return report, err
			}
			continue
		}
		_, ok := loop.Programs.Mitigator(match.PlaybookId)
		if !ok {
			if err := skip("no_mitigation"); err != nil {
				return report, err
			}
			continue
		}
		diagnoser, ok := loop.Programs.Diagnoser(match.PlaybookId)
		if !ok {
			if err := skip("diagnose_unavailable"); err != nil {
				return report, err
			}
			continue
		}
		view, err := loop.snapshotView(ctx)
		if err != nil {
			attempt.Error = err.Error()
			if err := skip("diagnose_unavailable"); err != nil {
				return report, err
			}
			continue
		}
		before := program.RunDiagnose(ctx, diagnoser, view, bindings, loop.deadline())
		if before.Result != program.Match {
			reason := "diagnose_unavailable"
			if before.Result == program.NoMatch {
				reason = "diagnose_no_longer_matches"
				observed := ToolResponse{}
				loop.verify(ctx, nil, &observed, true)
				updateStageFindings(state, observed)
			}
			if err := skip(reason); err != nil {
				return report, err
			}
			continue
		}
		if len(match.targets) == 0 {
			if err := skip("unexplained_coverage"); err != nil {
				return report, err
			}
			continue
		}
		// Pin the live diagnosis view for planning; never reuse the dispatch-time
		// view, and never invoke a working-checkout compiler in the automatic stage.
		planResponse := ToolResponse{}
		loop.matchedViews[matchedViewKey(match.PlaybookId, bindings)] = view
		delete(loop.matchedPlans, matchedViewKey(match.PlaybookId, bindings))
		if err := withoutResponderLocks(loop, "plan", true, func() {
			planResponse = loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: match.PlaybookId, Bindings: bindings, FindingFingerprints: match.targets, stage: true})
		}); err != nil {
			return report, err
		}
		if planResponse.Error != "" {
			if terminal := loop.terminalPlanError(match.PlaybookId); terminal != nil {
				attempt.Error = terminal.Error()
				if err := skip(stagePlanNotExecutable); err != nil {
					return report, err
				}
				continue
			}
			attempt.Result = "apply_error"
			attempt.Error = planResponse.Error
			if planResponse.FailureReason == "script_unprovenanced" {
				attempt.SkipReason = "script_unprovenanced"
			}
			if err := save(false); err != nil {
				return report, err
			}
			continue
		}
		ready = append(ready, stageReady{match: match, attempt: attempt, bindings: bindings, planID: planResponse.PlanID})
	}
	if err := stage.applyMatched(ctx, loop, state, ready, remaining, save); err != nil {
		return report, err
	}
	// Once all opening faults have verified repairs, let the remaining health
	// detectors' own windows roll over before handoff.
	if !state.Refresh {
		stage.awaitRepairedHealth(ctx, loop, state, report, remaining())
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	// Use the responder's completion verifier for final field equivalence;
	// application success alone does not grant reflection learning authority.
	var applications []RecordedPlanApplication
	if err := withoutResponderLocks(loop, "credit-equivalent-repairs", true, func() {
		loop.CreditEquivalentRepairs(ctx, loop.ApplicationRepairReceipts(state.RepairActions))
		applications = loop.PlanApplications()
	}); err != nil {
		return report, err
	}
	for _, attempt := range report.Mitigations {
		for _, application := range applications {
			if application.ID == attempt.GetApplyReceipt().GetApplicationId() {
				receipt, err := recordedPlanContract(application)
				if err != nil {
					return report, err
				}
				attempt.ApplyReceipt = receipt
			}
		}
	}
	if err := save(true); err != nil {
		return report, err
	}
	return report, nil
}

func stageAttemptKey(id string, bindings program.Bindings) string {
	return id + "\x00" + bindingKey(bindings)
}

func findingFingerprints(findings []sdk.Finding) []string {
	var result []string
	for _, finding := range findings {
		if finding.Status == sdk.FindingActive {
			result = append(result, FindingFingerprint(finding))
		}
	}
	sort.Strings(result)
	return result
}

// A candidate retains the exact live diagnosis bindings and its finding
// associations. Authored explanation links describe coverage, not bindability.
type stageCandidate struct {
	*contracts.PlaybookMatch
	targets []string
}

func stageCandidates(state *IncidentState) []stageCandidate {
	var matches []stageCandidate
	for _, match := range state.Coverage.GetMatches() {
		bindings := programBindingsState(match.Bindings)
		matches = append(matches, stageCandidate{PlaybookMatch: match, targets: matchedFindingFingerprints(state.Request.Findings, state.Request.InitialProgramRuns, match.PlaybookId, bindings)})
	}
	return matches
}

func updateStageFindings(state *IncidentState, response ToolResponse) {
	// Failed or unconfirmed observations cannot erase the opening findings.
	if response.Error != "" && len(response.DetectorStates) == 0 {
		return
	}
	confirmed := map[string]bool{}
	for _, evaluation := range response.DetectorStates {
		if evaluation.Status == DetectorEvaluationClear || evaluation.Status == DetectorEvaluationFiring {
			confirmed[evaluation.DetectorID] = true
		}
	}
	findings := append([]sdk.Finding(nil), response.Findings...)
	seen := map[string]bool{}
	for _, finding := range findings {
		seen[FindingFingerprint(finding)] = true
	}
	for _, finding := range state.Findings {
		if !confirmed[finding.DetectorID] && !seen[FindingFingerprint(finding)] {
			findings = append(findings, finding)
		}
	}
	sortFindings(findings)
	state.Findings = findings
	state.refreshUnexplained()
}

func recordedPlanContract(application RecordedPlanApplication) (*contracts.PlanApplication, error) {
	receipt, err := application.Result.Proto()
	if err != nil {
		return nil, fmt.Errorf("encode stage apply receipt: %w", err)
	}
	receipt.ApplicationId = application.ID
	receipt.ProgramIds = append([]string(nil), application.ProgramIDs...)
	receipt.StartedAt = optionalTimestamp(application.StartedAt)
	receipt.CompletedAt = optionalTimestamp(application.CompletedAt)
	receipt.Error = application.Error
	return receipt, nil
}

// Protobuf receipts contain oneofs; encoding/json cannot reconstruct them.
// This type owns that storage edge so all checkpoints use ProtoJSON.
type encodedResponseStageProgress struct {
	Report             json.RawMessage
	Coverage           json.RawMessage
	InitialProgramRuns []ProgramRun
	Complete           bool
}

func (progress responseStageProgress) MarshalJSON() ([]byte, error) {
	encoded := encodedResponseStageProgress{InitialProgramRuns: progress.InitialProgramRuns, Complete: progress.Complete}
	if progress.Report != nil {
		raw, err := protojson.Marshal(progress.Report)
		if err != nil {
			return nil, err
		}
		encoded.Report = raw
	}
	if progress.Coverage != nil {
		raw, err := protojson.Marshal(progress.Coverage)
		if err != nil {
			return nil, err
		}
		encoded.Coverage = raw
	}
	return json.Marshal(encoded)
}
func (progress *responseStageProgress) UnmarshalJSON(raw []byte) error {
	var encoded encodedResponseStageProgress
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return err
	}
	*progress = responseStageProgress{Complete: encoded.Complete, InitialProgramRuns: encoded.InitialProgramRuns}
	if len(encoded.Report) > 0 && string(encoded.Report) != "null" {
		progress.Report = &contracts.StageReport{}
		if err := protojson.Unmarshal(encoded.Report, progress.Report); err != nil {
			return err
		}
	}
	if len(encoded.Coverage) > 0 && string(encoded.Coverage) != "null" {
		progress.Coverage = &contracts.PlaybookCoverage{}
		if err := protojson.Unmarshal(encoded.Coverage, progress.Coverage); err != nil {
			return err
		}
	}
	return nil
}

func recordStageFindingProgress(attempt *contracts.StageMitigation, covered []string, findings []sdk.Finding) {
	remaining := map[string]bool{}
	for _, finding := range findings {
		remaining[FindingFingerprint(finding)] = true
	}
	attempt.RemainingFindingFingerprints = nil
	attempt.ClearedFindingFingerprints = nil
	for _, fp := range covered {
		if remaining[fp] {
			attempt.RemainingFindingFingerprints = append(attempt.RemainingFindingFingerprints, fp)
		} else {
			attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, fp)
		}
	}
}

// Only a completed, independently verified application may discharge its exact
// diagnosed finding targets. A fresh detector still reporting that fingerprint wins.
func clearVerifiedStageTargets(state *IncidentState, attempt *contracts.StageMitigation, covered []string, observed ToolResponse) {
	receipt := attempt.GetApplyReceipt()
	if !receipt.GetApplied() || receipt.GetDryRun() || receipt.GetRolledBack() || receipt.GetRolloutPending() != nil || receipt.GetError() != "" || receipt.GetRollbackError() != "" || !receipt.GetVerificationCompleted() || !receipt.GetVerificationPassed() || attempt.GetError() != "" || attempt.GetPostApplyDiagnose().GetError() != "" || attempt.GetPostApplyDiagnose().GetProgramId() != attempt.GetPlaybookId() || attempt.GetPostApplyDiagnose().GetProgramRevision() != attempt.GetProgramRevision() || !programBindingsState(attempt.GetBindings()).Equal(programBindingsState(attempt.GetPostApplyDiagnose().GetBindings())) || attempt.GetPostApplyDiagnose().GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
		return
	}
	clear := map[string]bool{}
	for _, fp := range covered {
		clear[fp] = true
	}
	for _, finding := range observed.Findings {
		delete(clear, FindingFingerprint(finding))
	}
	findings := make([]sdk.Finding, 0, len(state.Findings))
	for _, finding := range state.Findings {
		if !clear[FindingFingerprint(finding)] {
			findings = append(findings, finding)
		}
	}
	state.Findings = findings
	state.refreshUnexplained()
}

// Only new health observations after complete repairs may consume this wait.
// Original unresolved findings, skipped/failed repairs and incident findings
// require responder investigation immediately. The wait follows each remaining
// detector's clearing horizon (rolling window and cadence), bounded by the
// declared ceiling. Waiting never grants closure.
func (stage *DeterministicResponseStage) awaitRepairedHealth(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, left time.Duration) {
	if len(state.Findings) == 0 || len(report.Mitigations) == 0 || left <= 0 {
		return
	}
	for _, attempt := range report.Mitigations {
		if attempt.GetResult() != "applied_cleared" {
			return
		}
	}
	opening := map[string]bool{}
	for _, finding := range state.Request.Findings {
		opening[FindingFingerprint(finding)] = true
	}
	health := map[string]bool{}
	for _, detector := range loop.Detectors {
		health[detector.Spec().ID] = detector.Spec().Class == sdk.DetectorClassHealth
	}
	eligible := func() bool {
		for _, finding := range state.Findings {
			if opening[FindingFingerprint(finding)] || !health[finding.DetectorID] {
				return false
			}
		}
		return len(state.Findings) > 0
	}
	if !eligible() {
		return
	}
	view, err := loop.snapshotView(ctx)
	if err != nil {
		return
	}
	horizons := map[string]time.Duration{}
	for _, detector := range loop.Detectors {
		horizons[detector.Spec().ID] = sdk.ClearingHorizon(detector, view)
	}
	ceiling := left
	if stage.ObservationCeiling > 0 && stage.ObservationCeiling < ceiling {
		ceiling = stage.ObservationCeiling
	}
	interval := stage.ObservationInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	started := time.Now()
	observe, cancel := context.WithTimeout(ctx, ceiling)
	defer cancel()
	for eligible() {
		var horizon time.Duration
		for _, finding := range state.Findings {
			horizon = max(horizon, horizons[finding.DetectorID])
		}
		if time.Since(started) >= horizon {
			return
		}
		timer := time.NewTimer(interval)
		select {
		case <-observe.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		response := ToolResponse{}
		loop.verify(observe, nil, &response, true)
		updateStageFindings(state, response)
		if response.Error != "" {
			return
		}
	}
}

// ResponderStepPanicError reports a panic inside one step that ran with the
// responder loop locks released. The step fails with this error; the
// controller keeps running.
type ResponderStepPanicError struct {
	Step  string
	Value any
	Stack string
}

func (e *ResponderStepPanicError) Error() string {
	return fmt.Sprintf("responder step %s panicked: %v", e.Step, e.Value)
}

// withoutResponderLocks runs one step with the responder loop mutex released,
// and operationMu too when operation is set, and always reacquires them. It is
// the only way to open an unlock window (TestResponderLoopLocksAreDeferredOrHelped):
// without it, a panic in the step unwinds into the caller's deferred unlocks of
// already released mutexes, an unrecoverable fatal error.
func withoutResponderLocks(loop *ResponderLoop, step string, operation bool, run func()) (err error) {
	loop.mu.Unlock()
	if operation {
		loop.operationMu.Unlock()
	}
	defer func() {
		if operation {
			loop.operationMu.Lock()
		}
		loop.mu.Lock()
		if r := recover(); r != nil {
			err = &ResponderStepPanicError{Step: step, Value: r, Stack: string(debug.Stack())}
		}
	}()
	run()
	return nil
}

// locked runs fn while holding the responder loop mutex.
func (l *ResponderLoop) locked(fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fn()
}

// stageReady is one matched, planned binding waiting for the stage's apply.
type stageReady struct {
	match    stageCandidate
	attempt  *contracts.StageMitigation
	bindings program.Bindings
	planID   string
}

// applyMatched sends every planned binding through one apply and one rollout
// wait, bounded by what is left of the stage budget. The application receipt
// is the whole application; each attempt keeps its own plan, action range,
// diagnosis and result.
func (stage *DeterministicResponseStage) applyMatched(ctx context.Context, loop *ResponderLoop, state *IncidentState, ready []stageReady, remaining func() time.Duration, save func(bool) error) error {
	if len(ready) == 0 {
		return nil
	}
	if remaining() <= 0 {
		for _, item := range ready {
			item.attempt.Result, item.attempt.SkipReason = "skipped", "stage_budget"
			recordStageFindingProgress(item.attempt, item.match.targets, state.Findings)
		}
		return save(false)
	}
	planIDs := make([]string, 0, len(ready))
	selected := make([]selectedPlan, 0, len(ready))
	offset := 0
	for _, item := range ready {
		plan := loop.plans[item.planID]
		count := len(plan.plan.Actions)
		item.attempt.PlanId = item.planID
		item.attempt.PlanActionStart, item.attempt.PlanActionCount = int32(offset), int32(count)
		offset += count
		// Persist intent before mutation. If interrupted, a later attempt hands this
		// uncertainty to the agent instead of silently repeating a possible write.
		item.attempt.Result = "apply_error"
		item.attempt.Error = "application interrupted before a completed receipt; inspect current state and recorded actions"
		planIDs = append(planIDs, item.planID)
		selected = append(selected, plan)
	}
	if err := save(false); err != nil {
		return err
	}
	rolloutTimeout := remaining()
	if stage.RolloutTimeout > 0 && stage.RolloutTimeout < rolloutTimeout {
		rolloutTimeout = stage.RolloutTimeout
	}
	// Apply through the ordinary tool boundary: mutation fences, recorded
	// writes, immutable receipts and compensation have one owner.
	var applied ToolResponse
	if err := withoutResponderLocks(loop, "apply", true, func() {
		applied = loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: planIDs, stage: true, rolloutTimeout: rolloutTimeout})
	}); err != nil {
		return err
	}
	var receipt *contracts.PlanApplication
	for _, application := range loop.applicationsLog {
		if len(application.PlanIDs) == len(planIDs) && application.PlanIDs[0] == planIDs[0] {
			var err error
			if receipt, err = recordedPlanContract(application); err != nil {
				return err
			}
		}
	}
	// Pending rollouts still get one fresh diagnosis/detector observation.
	if applied.RolloutPending != nil {
		loop.verify(ctx, selected, &applied, true)
	}
	updateStageFindings(state, applied)
	for _, item := range ready {
		attempt := item.attempt
		attempt.Error = applied.Error
		attempt.ApplyReceipt = receipt
		for _, run := range applied.ProgramRuns {
			if run.ProgramID == item.match.PlaybookId && run.Bindings.Equal(item.bindings) {
				attempt.PostApplyDiagnose = programRunContract(run)
				break
			}
		}
		// Scoped executor verification and the bound post-write diagnosis are
		// newer evidence than a retained opening incident finding. An incident
		// detector may not be installed in this loop's health-only registry.
		clearVerifiedStageTargets(state, attempt, item.match.targets, applied)
		recordStageFindingProgress(attempt, item.match.targets, state.Findings)
		switch {
		case attempt.ApplyReceipt.GetRolloutPending().GetReasonCode() == "admission_blocked":
			attempt.Result = "admission_blocked"
		case attempt.ApplyReceipt.GetRolloutPending() != nil:
			attempt.Result = "rollout_pending_at_handoff"
		case !attempt.ApplyReceipt.GetApplied() || attempt.ApplyReceipt.GetRolledBack() || len(attempt.ApplyReceipt.GetActionsApplied()) < int(attempt.PlanActionStart+attempt.PlanActionCount):
			attempt.Result = "apply_error"
		case applied.Error != "":
			attempt.Result = "rollout_failed"
		case attempt.PostApplyDiagnose.GetVerdict().GetResult() == contracts.Verdict_RESULT_NO_MATCH && len(attempt.RemainingFindingFingerprints) == 0:
			attempt.Result = "applied_cleared"
		default:
			attempt.Result = "applied_still_matching"
		}
	}
	return save(false)
}

// attemptApplication narrows a shared stage application to one attempt's
// actions, so the repair resources it reports belong to that binding alone.
func attemptApplication(application RecordedPlanApplication, attempt *contracts.StageMitigation) RecordedPlanApplication {
	count := int(attempt.GetPlanActionCount())
	if count == 0 {
		return application
	}
	start := int(attempt.GetPlanActionStart())
	window := func(length int) (int, int) { return min(start, length), min(start+count, length) }
	result := application.Result
	low, high := window(len(result.Plan.Actions))
	result.Plan.Actions = result.Plan.Actions[low:high]
	low, high = window(len(result.ActionsApplied))
	result.ActionsApplied = result.ActionsApplied[low:high]
	low, high = window(len(result.ActionReports))
	result.ActionReports = result.ActionReports[low:high]
	application.Result = result
	return application
}
