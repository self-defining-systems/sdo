package runtime

import (
	"context"
	"encoding/json"
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

const DefaultStageRolloutTimeout = 30 * time.Second

// DefaultStageObservationCeiling bounds the post-repair wait for remaining
// health findings, whatever their detectors' clearing horizons.
const DefaultStageObservationCeiling = 60 * time.Second

// DeterministicResponseStage uses the same plan/apply and immutable receipt
// boundary as the responder, operating only on accepted registry programs.
type DeterministicResponseStage struct {
	NewLoop             func(IncidentRequest) *ResponderLoop
	Restore             func(context.Context, IncidentRequest, *ResponderLoop) error
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

func (*DeterministicResponseStage) Name() string { return "deterministic" }

func (stage *DeterministicResponseStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	report := &contracts.StageReport{Stage: stage.Name()}
	if stage.NewLoop == nil {
		return report, nil
	}
	loop := stage.NewLoop(state.Request)
	if loop == nil {
		return report, nil
	}
	defer loop.CloseWorkingPrograms()
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
	if progress.Report != nil {
		report = proto.Clone(progress.Report).(*contracts.StageReport)
		state.Coverage = cloneCoverage(progress.Coverage)
		state.Request.PlaybookCoverage = cloneCoverage(progress.Coverage)
		state.Request.InitialProgramRuns = append([]ProgramRun(nil), progress.InitialProgramRuns...)
		loop.Request = state.Request
	}
	save := func(complete bool) error {
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
			loop.verify(ctx, selected, &response)
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
		if progress.Complete {
			return report, save(true)
		}
	}
	if state.Coverage == nil {
		state.Request.InitialProgramRuns = loop.InitialDiagnose(ctx)
		fillProgramRevisions(ctx, state.Request.InitialProgramRuns, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, loop.Trust)
		state.Request.PlaybookCoverage = cloneCoverage(state.Coverage)
		loop.Request = state.Request
	}
	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	loop.mu.Lock()
	defer loop.mu.Unlock()
	priorWait, priorTimeout := loop.waitForStageRollouts, loop.Executor.RolloutTimeout
	defer func() { loop.waitForStageRollouts = priorWait; loop.Executor.RolloutTimeout = priorTimeout }()
	loop.waitForStageRollouts = true
	budget := stage.RolloutTimeout
	if budget <= 0 || budget > DefaultStageRolloutTimeout {
		budget = DefaultStageRolloutTimeout
	}
	loop.Executor.RolloutTimeout = budget
	matches := stageCandidates(loop, state)
	attempted := map[string]bool{}
	for _, attempt := range report.Mitigations {
		attempted[stageAttemptKey(attempt.PlaybookId, programBindingsState(attempt.Bindings))] = true
	}
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
		report.Mitigations = append(report.Mitigations, attempt)
		skip := func(reason string) error {
			attempt.SkipReason = reason
			recordStageFindingProgress(attempt, match.ExplainedFindingFingerprints, state.Findings)
			return save(false)
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
		mitigator, ok := loop.Programs.Mitigator(match.PlaybookId)
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
		if !completeFindingBindings(mitigator.Spec(), bindings, state.Request.Findings, match.ExplainedFindingFingerprints) || program.ValidateBindings(diagnoser.Spec(), bindings) != nil {
			if err := skip("unbound_parameter"); err != nil {
				return report, err
			}
			continue
		}
		if len(match.ExplainedFindingFingerprints) == 0 {
			if err := skip("unexplained_coverage"); err != nil {
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
				loop.verify(ctx, nil, &observed)
				updateStageFindings(state, observed)
			}
			if err := skip(reason); err != nil {
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
			planResponse = loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: match.PlaybookId, Bindings: bindings, FindingFingerprints: match.ExplainedFindingFingerprints})
		}); err != nil {
			return report, err
		}
		if planResponse.Error != "" {
			attempt.Result = "apply_error"
			attempt.Error = planResponse.Error
			if err := save(false); err != nil {
				return report, err
			}
			continue
		}
		attempt.PlanId = planResponse.PlanID
		// Persist intent before mutation. If interrupted, a later attempt hands this
		// uncertainty to the agent instead of silently repeating a possible write.
		attempt.Result = "apply_error"
		attempt.Error = "application interrupted before a completed receipt; inspect current state and recorded actions"
		if err := save(false); err != nil {
			return report, err
		}
		// Apply through the ordinary tool boundary: mutation fences, recorded
		// writes, immutable receipts and compensation have one owner.
		var applied ToolResponse
		if err := withoutResponderLocks(loop, "apply", true, func() {
			applied = loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planResponse.PlanID}})
		}); err != nil {
			return report, err
		}
		attempt.Error = applied.Error
		for _, application := range loop.applicationsLog {
			if len(application.PlanIDs) != 1 || application.PlanIDs[0] != planResponse.PlanID {
				continue
			}
			receipt, err := recordedPlanContract(application)
			if err != nil {
				return report, err
			}
			attempt.ApplyReceipt = receipt
		}
		// Pending rollouts still get one fresh diagnosis/detector observation.
		if applied.RolloutPending != nil {
			loop.verify(ctx, []selectedPlan{loop.plans[planResponse.PlanID]}, &applied)
		}
		updateStageFindings(state, applied)
		for _, run := range applied.ProgramRuns {
			if run.ProgramID == match.PlaybookId && run.Bindings.Equal(bindings) {
				attempt.PostApplyDiagnose = programRunContract(run)
				break
			}
		}
		// Scoped executor verification and the bound post-write diagnosis are
		// newer evidence than a retained opening incident finding. An incident
		// detector may not be installed in this loop's health-only registry.
		clearVerifiedStageTargets(state, attempt, match.ExplainedFindingFingerprints, applied)
		recordStageFindingProgress(attempt, match.ExplainedFindingFingerprints, state.Findings)
		switch {
		case attempt.ApplyReceipt.GetRolloutPending() != nil:
			attempt.Result = "rollout_pending_at_handoff"
		case !attempt.ApplyReceipt.GetApplied() || attempt.ApplyReceipt.GetRolledBack() || len(attempt.ApplyReceipt.GetActionsApplied()) < len(attempt.ApplyReceipt.GetPlan().GetActions()):
			attempt.Result = "apply_error"
		case applied.Error != "":
			attempt.Result = "rollout_failed"
		case attempt.PostApplyDiagnose.GetVerdict().GetResult() == contracts.Verdict_RESULT_NO_MATCH && len(attempt.RemainingFindingFingerprints) == 0:
			attempt.Result = "applied_cleared"
		default:
			attempt.Result = "applied_still_matching"
		}
		if err := save(false); err != nil {
			return report, err
		}
	}
	// Once all opening faults have verified repairs, let the remaining health
	// detectors' own windows roll over before handoff.
	stage.awaitRepairedHealth(ctx, loop, state, report)
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

// Bindings must be complete and reproducible from an authored covered finding.
// Scalar defaults alone do not supply a finding-derived binding.
func completeFindingBindings(spec program.ProgramSpec, bindings program.Bindings, findings []sdk.Finding, covered []string) bool {
	if program.ValidateBindings(spec, bindings) != nil {
		return false
	}
	for _, param := range spec.Params {
		_, object := bindings.Objects[param.Name]
		_, value := bindings.Values[param.Name]
		if !object && !value {
			return false
		}
	}
	for _, finding := range findings {
		for _, fp := range covered {
			if FindingFingerprint(finding) == fp {
				if bound, err := program.BindFinding(spec, finding); err == nil && bound.Equal(bindings) {
					return true
				}
			}
		}
	}
	return false
}

func stageCandidates(loop *ResponderLoop, state *IncidentState) []*contracts.PlaybookMatch {
	matches := append([]*contracts.PlaybookMatch(nil), state.Coverage.GetMatches()...)
	// Missing parameters can prevent a Match altogether. Preserve that skipped
	// decision for explicitly linked playbooks instead of dropping it silently.
	if loop.Programs != nil {
		for _, finding := range state.Request.Findings {
			for _, spec := range loop.Programs.List() {
				if !programExplainsFinding(finding, spec.ID) {
					continue
				}
				if bound, err := program.BindFinding(spec, finding); err != nil {
					matches = append(matches, &contracts.PlaybookMatch{PlaybookId: spec.ID, Bindings: programBindingsContract(bound), ExplainedFindingFingerprints: []string{FindingFingerprint(finding)}})
				}
			}
		}
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
// authored coverage. A fresh detector still reporting that fingerprint wins.
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
func (stage *DeterministicResponseStage) awaitRepairedHealth(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport) {
	if len(state.Findings) == 0 || len(report.Mitigations) == 0 {
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
	ceiling := stage.ObservationCeiling
	if ceiling <= 0 || ceiling > DefaultStageObservationCeiling {
		ceiling = DefaultStageObservationCeiling
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
		loop.verify(observe, nil, &response)
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
