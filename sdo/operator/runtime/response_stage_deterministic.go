package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"log/slog"
	"runtime/debug"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/workloadhealth"
)

// DefaultStageBudget retains the existing pass/refresh scheduling bound.
// Shared recovery uses DefaultMitigationRolloutTimeout; direct writes remain
// bounded by the original incident context.
const DefaultStageBudget = 45 * time.Second

const stagePlanNotExecutable = "plan_not_executable"

// stageIneffective skips a match whose program revision already applied in
// this incident for the same findings and left them open (or matching). Its
// replacement objects, such as the Pod a restart creates, are new bindings
// but not new evidence. A finding the earlier attempt did not target is.
const stageIneffective = "ineffective"

// stageRevertedIneffective is an applied attempt that cleared none of its
// findings while its diagnosis still matched, whose writes the stage restored
// before handoff so the responder starts from the original fault.
const stageRevertedIneffective = "reverted_ineffective"

// stageIneffectiveTargets maps a program revision to the findings it applied
// for and did not clear.
type stageIneffectiveTargets map[string]map[string]bool

func ineffectiveStageTargets(attempts []*contracts.StageMitigation) stageIneffectiveTargets {
	targets := stageIneffectiveTargets{}
	for _, attempt := range attempts {
		targets.add(attempt)
	}
	return targets
}

func (t stageIneffectiveTargets) add(attempt *contracts.StageMitigation) {
	if attempt.GetResult() != "applied_still_matching" && attempt.GetResult() != stageRevertedIneffective {
		return
	}
	key := attempt.GetPlaybookId() + "\x00" + attempt.GetProgramRevision()
	if t[key] == nil {
		t[key] = map[string]bool{}
	}
	for _, fp := range append(append([]string(nil), attempt.GetClearedFindingFingerprints()...), attempt.GetRemainingFindingFingerprints()...) {
		t[key][fp] = true
	}
}

func (t stageIneffectiveTargets) covers(id, revision string, targets []string) bool {
	seen := t[id+"\x00"+revision]
	if len(seen) == 0 || len(targets) == 0 {
		return false
	}
	for _, fp := range targets {
		if !seen[fp] {
			return false
		}
	}
	return true
}

// DeterministicResponseStage uses the same plan/apply and immutable receipt
// boundary as the responder, operating only on accepted registry programs.
type DeterministicResponseStage struct {
	NewLoop func(IncidentRequest) *ResponderLoop
	Restore func(context.Context, IncidentRequest, *ResponderLoop) error
	// KeepLoop leaves the loop NewLoop returns open: it belongs to a running
	// responder, so a later pass must not close its working programs.
	KeepLoop bool
	// Budget shortens shared observation (DefaultMitigationRolloutTimeout when unset).
	Budget time.Duration
	// RolloutTimeout caps apply convergence I/O, with one rollout sample.
	// ObservationCeiling shortens shared recovery; neither grants writes.
	RolloutTimeout      time.Duration
	ObservationInterval time.Duration
	ObservationCeiling  time.Duration
	recoveryNow         func() time.Time
	recoveryWait        func(context.Context, time.Duration) error
}

type responseStageProgress struct {
	RecoveryDeadline time.Time
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
				l.rememberTerminalPlanError(attempt.PlaybookId, attempt.ProgramRevision, programBindingsState(attempt.Bindings), &NonExecutablePlanError{Cause: errors.New(attempt.Error)})
			}
		}
	}
}

func (*DeterministicResponseStage) Name() string { return "deterministic" }

func (stage *DeterministicResponseStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	report := &contracts.StageReport{Stage: stage.Name(), StartedAt: contractTimestamp(lifecycleio.Now().UTC())}
	if stage.NewLoop == nil {
		return report, nil
	}
	loop := stage.NewLoop(state.Request)
	if loop == nil {
		return report, nil
	}
	report.StartedAt = contractTimestamp(loop.clock().UTC())
	startedAt := report.StartedAt
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
	if report.StartedAt == nil {
		report.StartedAt = startedAt
	}
	loop.restoreTerminalStagePlanErrors()

	save := func(complete bool) error {
		retainAdmissionStageFindings(state, report)
		report.RemainingFindingFingerprints = findingFingerprints(state.Findings)
		loop.stageProgress[stage.Name()] = responseStageProgress{Report: report, Coverage: cloneCoverage(state.Coverage), InitialProgramRuns: append([]ProgramRun(nil), state.Request.InitialProgramRuns...), Complete: complete, RecoveryDeadline: progress.RecoveryDeadline}
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
			if err := stage.awaitApplications(ctx, loop, state, report, &progress, save); err != nil {
				return report, err
			}
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
			fillProgramRevisionsWith(loop.gitRead(), ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
			runs = append(append([]ProgramRun(nil), state.Request.InitialProgramRuns...), runs...)
		} else {
			runs = loop.InitialDiagnose(ctx)
			fillProgramRevisionsWith(loop.gitRead(), ctx, runs, state.Request.RepositoryWorktree, state.Request.SourceCommit)
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
	var appliedHere []*contracts.StageMitigation
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
	ineffective := ineffectiveStageTargets(report.Mitigations)
	// Only diagnoses that wrote nothing receive a second turn, and only after a
	// turn that wrote or cleared something: an unchanged cluster would give the
	// same diagnoses again. Keep the opening order; dependencies are learned
	// from live state, never names.
	deferred := map[string]bool{}
	turnEnd, turnFindings, progressed := len(matches), len(state.Findings), false
	for index := 0; index < len(matches); index++ {
		if index == turnEnd {
			if !progressed && len(state.Findings) >= turnFindings {
				break
			}
			turnEnd, turnFindings, progressed = len(matches), len(state.Findings), false
		}
		match := matches[index]
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
		if terminal := loop.terminalPlanError(match.PlaybookId, bindings); terminal != nil {
			attempt.Error = terminal.Error()
			if err := skip(stagePlanNotExecutable); err != nil {
				return report, err
			}
			continue
		}
		if ineffective.covers(attempt.PlaybookId, attempt.ProgramRevision, match.targets) {
			if err := skip(stageIneffective); err != nil {
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
		// A Diagnose-only playbook still runs: an out-of-reach diagnosis
		// escalates without a mitigation.
		mitigator, canMitigate := loop.Programs.Mitigator(match.PlaybookId)
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
			if len(matches) > 1 && !deferred[key] {
				deferred[key] = true
				delete(attempted, key)
				retry[key] = attempt
				matches = append(matches, match)
			}
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
		if escalation, ok := stageEscalation(ctx, mitigator, canMitigate, view, bindings, before, loop.deadline()); ok {
			escalateStageAttempt(state, attempt, match, bindings, before, escalation)
			progressed = true
			if err := save(false); err != nil {
				return report, err
			}
			continue
		}
		if !canMitigate {
			if err := skip("no_mitigation"); err != nil {
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
			if terminal := loop.terminalPlanError(match.PlaybookId, bindings); terminal != nil {
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
		if loop.sameWriteInProgress(ctx, stage.stageNow(), report, loop.plans[planResponse.PlanID].plan) {
			if err := skip(stageAlreadyAttempted); err != nil {
				return report, err
			}
			continue
		}
		if err := stage.applyMatch(ctx, loop, state, stagePlanned{match: match, attempt: attempt, bindings: bindings, planID: planResponse.PlanID}, save); err != nil {
			return report, err
		}
		progressed = true
		appliedHere = append(appliedHere, attempt)
		ineffective.add(attempt)
		// An unsuccessful application is evidence about that binding, not a
		// reason to starve the other matches. Each later match diagnoses fresh
		// state and goes through the ordinary conflict and mutation fences.
	}
	if err := stage.awaitApplications(ctx, loop, state, report, &progress, save); err != nil {
		return report, err
	}
	if err := stage.revertIneffective(ctx, loop, state, report, appliedHere, save); err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	// Use the responder's completion verifier for final field equivalence;
	// application success alone does not grant reflection learning authority.
	var applications []RecordedPlanApplication
	if err := withoutResponderLocks(loop, "credit-equivalent-repairs", true, func() {
		loop.CreditEquivalentRepairs(ctx, loop.ApplicationRepairReceipts(state.RepairActions))
		applications = loop.ApplicationEvidence()
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
	// Namespace admission is a prerequisite for workload recovery. Prioritize
	// its covered fixes using the detector contract, never playbook names or
	// benchmark fault identities. Preserve order among peers.
	admission := map[string]bool{}
	for _, finding := range state.Findings {
		if finding.Status == sdk.FindingActive && finding.RuleID == workloadhealth.RulePodAdmissionRejected && finding.PrimaryResource.Kind == "Namespace" {
			admission[FindingFingerprint(finding)] = true
		}
	}
	priority := func(match stageCandidate) bool {
		for _, target := range match.targets {
			if admission[target] {
				return true
			}
		}
		return false
	}
	sort.SliceStable(matches, func(i, j int) bool { return priority(matches[i]) && !priority(matches[j]) })
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
	RecoveryDeadline   time.Time
	Report             json.RawMessage
	Coverage           json.RawMessage
	InitialProgramRuns []ProgramRun
	Complete           bool
}

func (progress responseStageProgress) MarshalJSON() ([]byte, error) {
	encoded := encodedResponseStageProgress{RecoveryDeadline: progress.RecoveryDeadline, InitialProgramRuns: progress.InitialProgramRuns, Complete: progress.Complete}
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
	*progress = responseStageProgress{RecoveryDeadline: encoded.RecoveryDeadline, Complete: encoded.Complete, InitialProgramRuns: encoded.InitialProgramRuns}
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

// Health observations after complete repairs may consume this wait, including
// opening traffic symptoms whose rolling windows still contain pre-repair errors.
// An incident finding that depends on a completely executed write and comes
// from a windowed detector may wait out that window once, counted from when the
// write took effect; any other incident finding, a failed or superseded repair,
// or a finding that outlives its window requires investigation.
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

// stagePlanned is one freshly diagnosed and planned binding.
type stagePlanned struct {
	match    stageCandidate
	attempt  *contracts.StageMitigation
	bindings program.Bindings
	planID   string
}

// applyMatch retains each binding's own receipt and compensation boundary.
// Its single rollout sample lets the other matches run while recovery progresses.
func (stage *DeterministicResponseStage) applyMatch(ctx context.Context, loop *ResponderLoop, state *IncidentState, item stagePlanned, save func(bool) error) error {
	plan := loop.plans[item.planID]
	item.attempt.PlanId = item.planID
	item.attempt.PlanActionCount = int32(len(plan.plan.Actions))
	// Persist intent before mutation. Interrupted writes are never replayed.
	item.attempt.Result = "apply_error"
	item.attempt.Error = "application interrupted before a completed receipt; inspect current state and recorded actions"
	planIDs := []string{item.planID}
	selected := []selectedPlan{plan}
	if err := save(false); err != nil {
		return err
	}
	// PollRollouts performs one rollout sample while preserving the normal
	// source/direct-readback I/O bound. A tiny rollout budget must not truncate
	// those reads; no serial recovery wait runs inside this apply.
	// Apply through the ordinary tool boundary: mutation fences, recorded
	// writes, immutable receipts and compensation have one owner.
	var applied ToolResponse
	if err := withoutResponderLocks(loop, "apply", true, func() {
		applied = loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: planIDs, stage: true, rolloutTimeout: stage.RolloutTimeout})
	}); err != nil {
		return err
	}
	// Pending rollouts still get one fresh diagnosis/detector observation.
	if applied.RolloutPending != nil {
		loop.verify(ctx, selected, &applied, true)
	}
	var receipt *contracts.PlanApplication
	complete := false
	for _, application := range loop.applicationsLog {
		if len(application.PlanIDs) == len(planIDs) && application.PlanIDs[0] == planIDs[0] {
			var err error
			if receipt, err = recordedPlanContract(application); err != nil {
				return err
			}
			scoped := attemptApplication(application, item.attempt).Result
			complete = PlanActionsComplete(scoped)
			item.attempt.TouchedObjects = touchedObjects(scoped)
		}
	}
	updateStageFindings(state, applied)
	{
		attempt := item.attempt
		attempt.Error = receipt.GetError()
		if receipt == nil {
			attempt.Error = applied.Error
		}
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
		case !attempt.ApplyReceipt.GetApplied() || attempt.ApplyReceipt.GetRolledBack() || !complete:
			attempt.Result = "apply_error"
		case attempt.Error != "":
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
	if start != 0 || count != len(application.Result.Plan.Actions) {
		refs := planCaptureRefs(result.Plan)
		result.ObjectEffects = nil
		for _, effect := range application.Result.ObjectEffects {
			if slices.Contains(refs, effect.Ref) {
				result.ObjectEffects = append(result.ObjectEffects, effect)
			}
		}
	}
	application.Result = result
	return application
}

// All admitted writes precede one shared recovery wait. Observation cannot
// select another write or replenish apply authority. Deadline survives restart.
func (stage *DeterministicResponseStage) awaitApplications(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, progress *responseStageProgress, save func(bool) error) (errOut error) {
	now := stage.recoveryNow
	if now == nil {
		now = lifecycleio.Now
	}
	wait := stage.recoveryWait
	if wait == nil {
		wait = func(ctx context.Context, d time.Duration) error {
			timer := lifecycleio.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	started := now()
	elapsed := report.GetRecoveryWait().GetDuration().AsDuration()
	exit := contracts.RecoveryWait_EXIT_REASON_FAILED
	// Every finding of this incident as last observed, and the ones a wait that
	// could not prove recovery hands back to the responder.
	seen := map[string]sdk.Finding{}
	for _, finding := range append(append([]sdk.Finding(nil), state.Request.Findings...), state.Findings...) {
		seen[FindingFingerprint(finding)] = finding
	}
	var held []sdk.Finding
	heldReason := ""
	// branch names why the wait ended, for the exit log.
	branch := ""
	defer func() {
		slog.Info("recovery_wait_exit", "incident_id", state.Request.IncidentID, "exit", exit.String(), "branch", branch)
		if exit != contracts.RecoveryWait_EXIT_REASON_CLEARED && len(state.Findings) == 0 && len(held) > 0 {
			// Detector clearance without proof is not recovery: the responder gets
			// the findings that cleared without it.
			slog.Info("recovery_wait_unproven", "incident_id", state.Request.IncidentID, "exit", exit.String(), "reason", heldReason)
			state.Findings = held
			state.refreshUnexplained()
		}
		observation := &contracts.RecoveryWait{ExitReason: exit, Duration: durationpb.New(elapsed + max(0, now().Sub(started)))}
		report.RecoveryWait = observation
		ids := map[string]bool{}
		for _, attempt := range report.Mitigations {
			if attempt.ApplyReceipt != nil {
				attempt.ApplyReceipt.RecoveryWait = proto.Clone(observation).(*contracts.RecoveryWait)
				ids[attempt.ApplyReceipt.ApplicationId] = true
			}
		}
		for _, entries := range [][]RecordedPlanApplication{loop.applicationsLog, loop.archivedApplications} {
			for i := range entries {
				if ids[entries[i].ID] {
					entries[i].Result.RecoveryWait = proto.Clone(observation).(*contracts.RecoveryWait)
				}
			}
		}
		for id := range ids {
			if result, ok := loop.applications[id]; ok {
				result.RecoveryWait = proto.Clone(observation).(*contracts.RecoveryWait)
				loop.applications[id] = result
			}
		}
		errOut = errors.Join(errOut, save(false))
	}()
	if progress.RecoveryDeadline.IsZero() {
		budget := DefaultMitigationRolloutTimeout
		if stage.Budget > 0 {
			budget = min(budget, stage.Budget)
		}
		if stage.ObservationCeiling > 0 {
			budget = min(budget, stage.ObservationCeiling)
		}
		progress.RecoveryDeadline = now().Add(budget)
		if deadline := state.Request.ResponseDeadline; !deadline.IsZero() && deadline.Before(progress.RecoveryDeadline) {
			progress.RecoveryDeadline = deadline
		}
		if err := save(false); err != nil {
			return err
		}
	}
	// Re-entry and legacy receipt reconstruction retain the original response
	// bound as well as the shared recovery bound.
	if deadline := state.Request.ResponseDeadline; !deadline.IsZero() && deadline.Before(progress.RecoveryDeadline) {
		progress.RecoveryDeadline = deadline
	}
	observe, cancel := context.WithDeadline(ctx, progress.RecoveryDeadline)
	defer cancel()
	interval := stage.ObservationInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	effects := recoveryEffects{}
	// When this wait first saw each open finding; a finding on a component no
	// stage write touched hands off once it outlasts its clearing horizon.
	for {
		pending := false
		applied := false
		var settling []*contracts.StageMitigation
		for _, attempt := range report.Mitigations {
			if reason := attempt.GetApplyReceipt().GetRolloutPending().GetReasonCode(); reason == "rollout_stalled" || reason == "admission_blocked" {
				exit = contracts.RecoveryWait_EXIT_REASON_BLOCKER
				return nil
			}
			if attempt.GetApplyReceipt().GetApplied() && !attempt.GetApplyReceipt().GetRolledBack() {
				applied = true
			}
			switch attempt.Result {
			case "rollout_pending_at_handoff":
				pending = true
			case "applied_cleared", "skipped":
			case "applied_still_matching":
				// A write still taking effect, or one whose remaining findings only wait
				// for their detectors to re-observe it, is checked below once a view exists.
				if !writeVerifiedForSettling(attempt) && !writeTakingEffect(attempt) {
					branch = "write refuted: " + attempt.GetPlaybookId() + " " + attempt.GetApplyReceipt().GetSettlementRejectionReason().String()
					return nil
				}
				if attempt.GetApplyReceipt().GetRolloutPending() != nil {
					pending = true
					continue
				}
				settling = append(settling, attempt)
			default:
				// Real failure or a verified dependency needs another actor.
				branch = "attempt " + attempt.GetPlaybookId() + " " + attempt.Result
				return nil
			}
		}
		if !applied && len(state.Findings) > 0 {
			exit = contracts.RecoveryWait_EXIT_REASON_NOTHING_APPLIED
			return nil
		}
		if !applied && !pending && len(state.Findings) == 0 {
			// No write of this stage to prove: detector clearance decides.
			exit = contracts.RecoveryWait_EXIT_REASON_CLEARED
			return nil
		}
		if recoveryTimeLeft(stage, progress.RecoveryDeadline, 0) == 0 {
			exit = contracts.RecoveryWait_EXIT_REASON_DEADLINE
			return ctx.Err()
		}
		view, viewErr := loop.snapshotView(observe)
		if viewErr != nil {
			// The stage cannot observe recovery, so another actor takes over; say why.
			slog.Warn("recovery_wait_snapshot_unavailable", "incident_id", state.Request.IncidentID, "error", viewErr)
			if len(held) == 0 {
				held, heldReason = sortedFindings(seen), "recovery could not be observed"
			}
			return nil
		}
		for _, finding := range state.Findings {
			seen[FindingFingerprint(finding)] = finding
		}
		state.Findings = retireDisappearedRecoveryEffects(view, state.Findings)
		for fp, effect := range stageLifecycleEffects(view, loop.applicationEvidenceLocked(), state.Findings) {
			effects[fp] = effect
		}
		state.Findings = effects.observe(view, state.Findings)
		state.refreshUnexplained()
		if !pending {
			check := loop.stageRecoveryCheck(observe, view, state)
			horizonOf := func(id string) (horizon, window time.Duration) {
				for _, detector := range loop.Detectors {
					if detector.Spec().ID == id {
						horizon = detectorClearingHorizon(detector, view) + 2*detector.Spec().Interval
						if detectorWindow(detector, view) > 0 {
							window = horizon
						}
					}
				}
				return max(horizon, 2*interval), window
			}
			// A program that still diagnoses its fault after its write is not
			// credited with recovery, whatever the members show; it gets the
			// longest rolling window among the incident's detectors, since its
			// own diagnosis may read the same lagging evidence.
			matching := ""
			for _, attempt := range report.Mitigations {
				if receipt := attempt.GetApplyReceipt(); receipt.GetApplied() && !receipt.GetRolledBack() &&
					attempt.GetPostApplyDiagnose().GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH {
					matching = "program " + attempt.GetPlaybookId() + " still diagnoses the fault after its write"
				}
			}
			if len(state.Findings) == 0 {
				switch decideStageRecovery(check, matching) {
				case stageCleared:
					exit = contracts.RecoveryWait_EXIT_REASON_CLEARED
					promoteSettledAttempts(settling)
					return nil
				default:
					held, heldReason = sortedFindings(seen), check.reason
					if matching != "" {
						heldReason = matching
					}
				}
			}
			var settled time.Duration
			if since := check.record.GetSince(); check.record.GetWritesSettled() && since != nil {
				settled = now().Sub(since.AsTime())
			} else {
				settled = -1
			}
			for _, finding := range state.Findings {
				fp := FindingFingerprint(finding)
				horizon, _ := horizonOf(finding.DetectorID)
				if settled >= horizon {
					branch = "finding " + fp + " persisted past its horizon after the stage's writes settled"
					return nil
				}
			}
			if check.verdict == recoveryPending && len(state.Findings) == 0 && settled >= check.horizon+2*interval {
				branch = "not recovered within its members' horizon: " + check.reason
				return nil
			}
			if matching != "" && check.verdict != recoveryPending && len(state.Findings) == 0 {
				var window time.Duration
				for _, finding := range seen {
					_, w := horizonOf(finding.DetectorID)
					window = max(window, w)
				}
				if settled >= window {
					branch = matching
					return nil
				}
			}
		}
		left := progress.RecoveryDeadline.Sub(now())
		if left <= 0 {
			exit = contracts.RecoveryWait_EXIT_REASON_DEADLINE
			return nil
		}
		if err := wait(observe, min(interval, left)); err != nil {
			if observe.Err() != nil && ctx.Err() == nil {
				exit = contracts.RecoveryWait_EXIT_REASON_DEADLINE
				return nil
			}
			return err
		}
		if observe.Err() != nil || recoveryTimeLeft(stage, progress.RecoveryDeadline, 0) == 0 {
			exit = contracts.RecoveryWait_EXIT_REASON_DEADLINE
			return ctx.Err()
		}
		response := ToolResponse{}
		loop.verify(observe, nil, &response, true)
		updateStageFindings(state, response)
		_, stages := loop.settleRecoveryApplicationsLocked(observe, IncidentResult{PlanApplications: loop.applicationEvidenceLocked()}, []*contracts.StageReport{report})
		if len(stages) > 0 {
			proto.Reset(report)
			proto.Merge(report, stages[0])
		}
		// Scoped settlement retires only the targets with bound recovery proof.
		for _, attempt := range report.Mitigations {
			if attempt.Result == "applied_cleared" {
				clearVerifiedStageTargets(state, attempt, attempt.ClearedFindingFingerprints, response)
			}
		}
		if err := save(false); err != nil {
			return err
		}
	}
}

// writeVerifiedForSettling reports an applied, unreverted write whose scoped verification passed and
// whose program no longer matches, so any remaining finding is waiting on detector observation.
// A confirmed non-object effect counts as verified: no object read can verify it.
func writeVerifiedForSettling(attempt *contracts.StageMitigation) bool {
	receipt := attempt.GetApplyReceipt()
	return receipt.GetApplied() && !receipt.GetRolledBack() && (receipt.GetVerificationPassed() || nonObjectEffectConfirmed(attempt)) &&
		attempt.GetPostApplyDiagnose().GetVerdict().GetResult() == contracts.Verdict_RESULT_NO_MATCH
}

// nonObjectEffectConfirmed reports an applied, unreverted attempt whose own
// actions changed state outside Kubernetes objects and each recorded its
// success (planwire.NonObjectEffect, nonObjectEffectsSucceeded). Such an
// effect touches no object a finding could be joined to, so the findings the
// attempt was matched against are its dependents.
func nonObjectEffectConfirmed(attempt *contracts.StageMitigation) bool {
	receipt := attempt.GetApplyReceipt()
	if !receipt.GetApplied() || receipt.GetRolledBack() || receipt.GetDryRun() || receipt.GetRollbackError() != "" {
		return false
	}
	actions, reports := receipt.GetPlan().GetActions(), receipt.GetActionReports()
	if count := int(attempt.GetPlanActionCount()); count > 0 {
		start := int(attempt.GetPlanActionStart())
		if start < 0 || start+count > len(actions) || start+count > len(reports) {
			return false
		}
		actions, reports = actions[start:start+count], reports[start:start+count]
	}
	return nonObjectEffectsSucceeded(actions, reports)
}

// writeTakingEffect reports an applied, unreverted, completely executed write
// that settlement has not refuted: its rollout may still be in progress, or the
// only objections are health or a diagnosis that may read the same windowed
// evidence. Superseded, failed, partial or unobservable writes are refuted.
func writeTakingEffect(attempt *contracts.StageMitigation) bool {
	receipt := attempt.GetApplyReceipt()
	if !receipt.GetApplied() || receipt.GetRolledBack() || receipt.GetDryRun() || receipt.GetRollbackError() != "" {
		return false
	}
	if len(receipt.GetActionReports()) != len(receipt.GetPlan().GetActions()) {
		return false
	}
	for _, report := range receipt.GetActionReports() {
		if report.GetStatus() != "applied" || report.GetFailureReason() != "" || report.GetRecordingError() != "" {
			return false
		}
	}
	switch receipt.GetSettlementRejectionReason() {
	case contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED,
		contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_ROLLOUT_PENDING,
		contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_HEALTH_NOT_CLEAR,
		contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_DIAGNOSIS_NOT_CLEAR:
		return true
	}
	return false
}

// stageDecision is what the stage's wait does once every finding is clear.
type stageDecision int

const (
	stageWait stageDecision = iota
	stageCleared
)

// decideStageRecovery applies Recovered to a stage whose findings have all
// cleared: it clears (the responder is skipped) when the opening alerts
// recovered. A program that still diagnoses its fault, or opening user tests
// that have not passed since the stage's writes settled, keep it waiting; the
// stage budget then hands off.
func decideStageRecovery(check recoveryCheck, matching string) stageDecision {
	if matching != "" || check.verdict == recoveryPending {
		return stageWait
	}
	return stageCleared
}

// promoteSettledAttempts records attempts whose remaining findings cleared on
// their own as cleared, when the program's latest diagnosis no longer matches.
// A program that still matches after recovery is not credited with it.
func promoteSettledAttempts(attempts []*contracts.StageMitigation) {
	for _, attempt := range attempts {
		if attempt.GetPostApplyDiagnose().GetVerdict().GetResult() != contracts.Verdict_RESULT_NO_MATCH {
			continue
		}
		attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, attempt.RemainingFindingFingerprints...)
		attempt.RemainingFindingFingerprints = nil
		attempt.Result = "applied_cleared"
	}
}

func recoveryTimeLeft(stage *DeterministicResponseStage, deadline time.Time, legacy time.Duration) time.Duration {
	if deadline.IsZero() {
		return max(0, legacy)
	}
	now := stage.recoveryNow
	if now == nil {
		now = lifecycleio.Now
	}
	return max(0, deadline.Sub(now()))
}

func sortedFindings(byFingerprint map[string]sdk.Finding) []sdk.Finding {
	findings := make([]sdk.Finding, 0, len(byFingerprint))
	for _, finding := range byFingerprint {
		findings = append(findings, finding)
	}
	sortFindings(findings)
	return findings
}
