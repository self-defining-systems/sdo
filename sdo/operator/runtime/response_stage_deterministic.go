package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const DefaultStageRolloutTimeout = 30 * time.Second

// DeterministicResponseStage uses the same plan/apply and immutable receipt
// boundary as the responder, operating only on accepted registry programs.
type DeterministicResponseStage struct {
	NewLoop        func(IncidentRequest) *ResponderLoop
	Restore        func(context.Context, IncidentRequest, *ResponderLoop) error
	RolloutTimeout time.Duration
}

type responseStageProgress struct {
	Report   *contracts.StageReport
	Complete bool
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
	defer func() { state.RepairActions = loop.ApplicationRepairReceipts(state.RepairActions) }()
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
	}
	save := func(complete bool) error {
		report.RemainingFindingFingerprints = findingFingerprints(state.Findings)
		loop.stageProgress[stage.Name()] = responseStageProgress{Report: report, Complete: complete}
		return loop.persistToolState(ctx)
	}
	// Always reobserve after recovery. A recorded attempt is never replayed,
	// including one interrupted between the write and its completed receipt.
	if progress.Report != nil {
		response := ToolResponse{}
		loop.verify(ctx, nil, &response)
		updateStageFindings(state, response)
		if progress.Complete {
			return report, nil
		}
	}
	if state.Coverage == nil {
		state.Request.InitialProgramRuns = loop.InitialDiagnose(ctx)
		fillProgramRevisions(ctx, state.Request.InitialProgramRuns, state.Request.RepositoryWorktree, state.Request.SourceCommit)
		state.Coverage = buildPlaybookCoverage(state.Findings, state.Request.InitialProgramRuns, loop.lastDiagnosisView, nil)
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
			attempt.RemainingFindingFingerprints = append([]string(nil), match.ExplainedFindingFingerprints...)
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
		loop.plan(ctx, ToolRequest{ProgramID: match.PlaybookId, Bindings: bindings, FindingFingerprints: match.ExplainedFindingFingerprints}, &planResponse)
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
		loop.mu.Unlock()
		loop.operationMu.Unlock()
		applied := loop.Handle(ctx, ToolRequest{Command: "apply", PlanIDs: []string{planResponse.PlanID}})
		loop.operationMu.Lock()
		loop.mu.Lock()
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
		remaining := map[string]bool{}
		for _, finding := range state.Findings {
			remaining[FindingFingerprint(finding)] = true
		}
		for _, fp := range match.ExplainedFindingFingerprints {
			if remaining[fp] {
				attempt.RemainingFindingFingerprints = append(attempt.RemainingFindingFingerprints, fp)
			} else {
				attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, fp)
			}
		}
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
func (progress responseStageProgress) MarshalJSON() ([]byte, error) {
	var report json.RawMessage
	if progress.Report != nil {
		raw, err := protojson.Marshal(progress.Report)
		if err != nil {
			return nil, err
		}
		report = raw
	}
	return json.Marshal(struct {
		Report   json.RawMessage
		Complete bool
	}{report, progress.Complete})
}
func (progress *responseStageProgress) UnmarshalJSON(raw []byte) error {
	var encoded struct {
		Report   json.RawMessage
		Complete bool
	}
	if err := json.Unmarshal(raw, &encoded); err != nil {
		return err
	}
	progress.Complete = encoded.Complete
	if len(encoded.Report) > 0 && string(encoded.Report) != "null" {
		progress.Report = &contracts.StageReport{}
		return protojson.Unmarshal(encoded.Report, progress.Report)
	}
	return nil
}
