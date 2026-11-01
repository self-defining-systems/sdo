package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

// DeterministicStage applies saved playbook programs whose diagnosis matches
// the live cluster, before any agent runs. Programs run in the memory host:
// the stage asks the registry for a diagnosis and a plan, and the controller
// alone applies, verifies and records the plan. A host error or disconnect is
// an attempt that did not happen, never clearance and never an empty plan.
//
// A program is applied only when it has exactly one entry point, a mitigator,
// and every parameter binds from the findings (the live diagnosis bindings).
// Success and failure counts are information; there is no trust gate.
type DeterministicStage struct {
	NewLoop func(IncidentRequest) *ResponderLoop
}

func (*DeterministicStage) Name() string { return "deterministic" }

type stageCandidate struct {
	programID string
	revision  string
	bindings  program.Bindings
	targets   []string
}

func (s *DeterministicStage) Run(ctx context.Context, state *IncidentState) (*contracts.StageReport, error) {
	report := &contracts.StageReport{Stage: s.Name()}
	if s.NewLoop == nil {
		return report, nil
	}
	loop := s.NewLoop(state.Request)
	if loop == nil {
		return report, nil
	}
	defer func() {
		state.RepairActions = ProgramRepairReceipts(loop.PlanApplications(), state.RepairActions)
		state.ProgramToolCalls = loop.ToolCalls()
	}()
	attempted := map[string]bool{}
	// A write already recorded for this incident was started by an earlier run
	// of the stage (or the responder) and is never replayed, including one that
	// stopped between the write and its completed receipt.
	written := false
	loop.locked(func() { written = loop.batches > 0 || loop.stageWrites > 0 })
	if written {
		reobserveStage(ctx, loop, state)
	}
	for _, candidate := range stageCandidates(state) {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		key := candidate.programID + "\x00" + bindingKey(candidate.bindings)
		if attempted[key] {
			continue
		}
		attempted[key] = true
		attempt := &contracts.StageMitigation{
			PlaybookId: candidate.programID, ProgramRevision: candidate.revision, Bindings: stageBindingsContract(candidate.bindings),
			Result: "skipped", TargetFindingFingerprints: candidate.targets,
		}
		report.Mitigations = append(report.Mitigations, attempt)
		if written {
			attempt.SkipReason = "already_attempted"
			continue
		}
		s.attempt(ctx, loop, state, candidate, attempt)
	}
	return report, nil
}

// stageCandidates lists the bindings the open-time diagnosis matched, each with
// the findings it was bound from, in diagnosis order.
func stageCandidates(state *IncidentState) []stageCandidate {
	var result []stageCandidate
	index := map[string]int{}
	for _, run := range state.Request.InitialProgramRuns {
		if run.Verdict.Result != program.Match || run.FindingFingerprint == "" || (run.CanMitigate != nil && !*run.CanMitigate) {
			continue
		}
		key := run.ProgramID + "\x00" + bindingKey(run.Bindings)
		at, ok := index[key]
		if !ok {
			at = len(result)
			index[key] = at
			result = append(result, stageCandidate{programID: run.ProgramID, revision: run.ProgramRevision, bindings: cloneProgramBindings(run.Bindings)})
		}
		result[at].targets = append(result[at].targets, run.FindingFingerprint)
	}
	for i := range result {
		sort.Strings(result[i].targets)
	}
	return result
}

func (s *DeterministicStage) attempt(ctx context.Context, loop *ResponderLoop, state *IncidentState, candidate stageCandidate, attempt *contracts.StageMitigation) {
	skip := func(reason string, err error) {
		attempt.SkipReason = reason
		if err != nil {
			attempt.Error = err.Error()
		}
	}
	if state.Request.RepairPolicy != "recorded-actions" {
		skip("repair_policy", nil)
		return
	}
	if loop.Programs == nil {
		skip("no_mitigation", errors.New("no programs are compiled"))
		return
	}
	entryPoints := 0
	for _, spec := range loop.Programs.List() {
		if spec.ID == candidate.programID {
			entryPoints++
		}
	}
	if entryPoints > 1 {
		skip("multiple_entry_points", nil)
		return
	}
	mitigator, ok := loop.Programs.Mitigator(candidate.programID)
	if !ok {
		skip("no_mitigation", nil)
		return
	}
	if err := program.ValidateBindings(mitigator.Spec(), candidate.bindings); err != nil {
		skip("unbound_parameter", err)
		return
	}
	diagnoser, ok := loop.Programs.Diagnoser(candidate.programID)
	if !ok {
		skip("diagnose_unavailable", nil)
		return
	}
	var verdict program.Verdict
	var viewErr error
	key := matchedViewKey(candidate.programID, candidate.bindings)
	loop.locked(func() {
		var view program.ReadView
		view, viewErr = loop.snapshotView(ctx)
		if viewErr != nil {
			return
		}
		verdict = program.RunDiagnose(ctx, diagnoser, view, candidate.bindings, loop.deadline())
		// Plan from the live view this diagnosis ran on, never the open-time one.
		loop.matchedViews[key] = view
		delete(loop.matchedPlans, key)
	})
	if viewErr != nil {
		skip("diagnose_unavailable", viewErr)
		return
	}
	switch verdict.Result {
	case program.Match:
	case program.NoMatch:
		skip("diagnose_no_longer_matches", nil)
		return
	default:
		skip("diagnose_unavailable", nil)
		return
	}
	planned := loop.Handle(ctx, ToolRequest{Command: "plan", ProgramID: candidate.programID, Bindings: candidate.bindings})
	if planned.Error != "" {
		attempt.Result, attempt.Error = "apply_error", planned.Error
		return
	}
	attempt.PlanId = planned.PlanID
	s.apply(ctx, loop, state, candidate, attempt)
}

// apply writes the planned program once and observes the result. The attempt
// stays "apply_error" until a completed receipt proves otherwise, so a stop
// between the write and its receipt is never replayed as a clean skip.
func (s *DeterministicStage) apply(ctx context.Context, loop *ResponderLoop, state *IncidentState, candidate stageCandidate, attempt *contracts.StageMitigation) {
	attempt.Result = "apply_error"
	attempt.Error = "application interrupted before a completed receipt"
	application, response := loop.stageApply(ctx, attempt.PlanId)
	if application.ID != "" {
		receipt, err := application.Result.Proto()
		if err == nil {
			receipt.ApplicationId = application.ID
			receipt.ProgramIds = application.ProgramIDs
			attempt.ApplyReceipt = receipt
		}
	}
	if response.Error != "" {
		attempt.Error = response.Error
		return
	}
	attempt.Error = ""
	remaining := map[string]bool{}
	for _, finding := range response.Findings {
		remaining[FindingFingerprint(finding)] = true
	}
	for _, fingerprint := range candidate.targets {
		if remaining[fingerprint] {
			attempt.RemainingFindingFingerprints = append(attempt.RemainingFindingFingerprints, fingerprint)
		} else {
			attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, fingerprint)
		}
	}
	stillMatching := false
	for _, run := range response.ProgramRuns {
		if run.ProgramID == candidate.programID && bindingKey(run.Bindings) == bindingKey(candidate.bindings) {
			attempt.PostApplyDiagnose = programRunContract(run)
			stillMatching = run.Verdict.Result != program.NoMatch
		}
	}
	attempt.Result = "applied_still_matching"
	if len(attempt.RemainingFindingFingerprints) == 0 && !stillMatching && attempt.PostApplyDiagnose != nil && application.Result.VerificationPassed {
		attempt.Result = "applied_cleared"
	}
	state.Findings, state.DetectorStates, state.Healthy = response.Findings, response.DetectorStates, response.Healthy
}

func stageBindingsContract(bindings program.Bindings) []*contracts.Binding {
	names := make([]string, 0, len(bindings))
	for name := range bindings {
		names = append(names, name)
	}
	sort.Strings(names)
	var result []*contracts.Binding
	for _, name := range names {
		result = append(result, &contracts.Binding{Name: name, Object: objectRefContract(bindings[name])})
	}
	return result
}

// stageApply applies one prepared plan on the stage's behalf. Unlike the
// responder's apply it spends no response-loop budget and never rolls back on
// its own: the stage decides what to keep from independent observation.
func (l *ResponderLoop) stageApply(ctx context.Context, planID string) (RecordedPlanApplication, ToolResponse) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	response := ToolResponse{Command: "apply"}
	plan, ok := l.plans[planID]
	if !ok {
		response.Error = fmt.Sprintf("unknown plan %q", planID)
		return RecordedPlanApplication{}, response
	}
	l.stageWrites++
	id := fmt.Sprintf("stage-%s-%d", l.sessionID, len(l.applicationsLog)+1)
	started := l.clock().UTC()
	entry := RecordedPlanApplication{ID: id, ProgramIDs: []string{plan.programID}, PlanIDs: []string{planID}, Result: PlanApplicationResult{Plan: plan.plan}, StartedAt: started, Pending: true}
	l.applicationsLog = append(l.applicationsLog, entry)
	if err := l.persistToolState(ctx); err != nil {
		l.applicationsLog = l.applicationsLog[:len(l.applicationsLog)-1]
		l.stageWrites--
		response.Error = err.Error()
		return RecordedPlanApplication{}, response
	}
	result, err := l.Executor.Apply(ctx, l.Request, plan.plan, false)
	if result.Applied || result.PendingAction != nil {
		l.applications[id] = result
	}
	if err != nil {
		response.Error = err.Error()
		l.addFailureSignal("stage_apply_failure:" + plan.programID)
	}
	if result.Applied {
		l.verify(ctx, []selectedPlan{plan}, &response)
	}
	entry.Result, entry.CompletedAt, entry.Error = result, l.clock().UTC(), response.Error
	entry.Pending = result.PendingAction != nil || result.RollingBack
	entry.RolledBack, entry.RollbackError = result.RolledBack, result.RollbackError
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == id {
			l.applicationsLog[i] = entry
		}
	}
	if perr := l.persistToolState(ctx); perr != nil && response.Error == "" {
		response.Error = perr.Error()
	}
	return entry, response
}

// reobserveStage refreshes the open findings without writing. It only accepts
// an observation that completed without error.
func reobserveStage(ctx context.Context, loop *ResponderLoop, state *IncidentState) {
	response := ToolResponse{}
	loop.locked(func() { loop.verify(ctx, nil, &response) })
	if response.Error != "" {
		return
	}
	state.Findings, state.DetectorStates, state.Healthy = response.Findings, response.DetectorStates, response.Healthy
}

// locked runs fn while holding the responder loop's locks.
func (l *ResponderLoop) locked(fn func()) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	fn()
}
