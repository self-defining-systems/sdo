package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type menuEntry struct {
	programID string
	bindings  program.Bindings
	plan      *program.Plan
}

func menuDigest(value any) string {
	data, _ := json.Marshal(value)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
func menuActionID(attempt string, round uint32, index int) string {
	return fmt.Sprintf("%s-r%d-a%d", attempt, round, index)
}
func menuActionReceipt(id string, ref sdk.ObjectRef) *contracts.MitigationActionReceipt {
	return &contracts.MitigationActionReceipt{ActionId: id, Resource: planRefProto(ref), Effect: contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION, State: contracts.MitigationActionReceipt_STATE_UNKNOWN}
}
func menuBindings(bindings program.Bindings) []*contracts.Binding {
	return programRunContract(ProgramRun{Bindings: bindings}).Bindings
}
func (l *ResponderLoop) listMenu(ctx context.Context, request ToolRequest, response *ToolResponse) {
	l.openMenuSession(ctx, response)
	if response.Error != "" {
		return
	}
	if !l.MenuConfig.valid() {
		response.Error = "menu safety capabilities unavailable"
		return
	}
	refusal := ""
	if l.MenuConfig.Refusal != nil {
		refusal = l.MenuConfig.Refusal()
	}
	entries := map[string]menuEntry{}
	var view sdk.DetectionContext
	if refusal == "" {
		var err error
		view, err = l.menuSnapshot(ctx)
		if err != nil {
			response.Error = "menu observation refused"
			return
		}
	}
	readView := menuReadView{DetectionContext: view}
	if refusal != "" && l.Programs != nil {
		// Catalog-only disabled options explain missing evidence without guessing a
		// diagnosis or fetching unapproved opaque collections.
		for _, spec := range l.Programs.List() {
			if _, ok := l.Programs.Mitigator(spec.ID); ok && (request.ProgramID == "" || request.ProgramID == spec.ID) {
				entries["unavailable-"+spec.ID] = menuEntry{programID: spec.ID}
			}
		}
	}
	if refusal == "" && l.Programs != nil {
		runs := SelectProgramCandidatesWithOptions(ctx, l.Programs, readView, l.Request.Findings, CandidateOptions{Deadline: l.deadline(), SweepLimit: l.CandidateSweepLimit})
		for _, run := range runs {
			if run.Verdict.Result != program.Match || request.ProgramID != "" && request.ProgramID != run.ProgramID {
				continue
			}
			if _, ok := l.Programs.Mitigator(run.ProgramID); !ok {
				continue
			}
			if len(request.Bindings) > 0 && !sameBindings(request.Bindings, run.Bindings) {
				continue
			}
			id := "program-" + menuDigest([]any{run.ProgramID, run.Bindings})
			entries[id] = menuEntry{programID: run.ProgramID, bindings: cloneProgramBindings(run.Bindings)}
		}
	}
	for _, plan := range l.MenuConfig.Plans {
		p := plan.Plan
		entries[plan.ID] = menuEntry{programID: plan.ProgramID, bindings: cloneProgramBindings(plan.Bindings), plan: &p}
	}
	s := l.selection.snapshot()
	menu := &contracts.MitigationMenu{Version: 1, IncidentId: s.IncidentId, SessionId: s.SessionId, DispatchEpoch: s.DispatchEpoch, SourceCommit: l.Request.SourceCommit, ScopeDigest: menuDigest(l.MenuConfig.Dependencies), PolicyDigest: l.MenuConfig.Safety.Digest, RoundLimit: s.TotalBatches - s.UsedBatches - s.ReservedBatches, OriginalDeadline: s.OriginalDeadline, RemainingBatches: s.TotalBatches - s.UsedBatches - s.ReservedBatches}
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		entry := entries[id]
		revision := l.Request.SourceCommit
		if entry.plan != nil {
			revision = menuDigest(entry.plan)
		} else if resolved, err := programDependencyRevision(ctx, l.Request.RepositoryWorktree, l.Request.SourceCommit, entry.programID); err == nil {
			revision = resolved
		}
		option := &contracts.MitigationOption{OptionId: id, ProgramId: entry.programID, ProgramRevision: revision, DependencyRevision: revision, Effects: []contracts.MitigationOption_Effect{contracts.MitigationOption_EFFECT_SDK_PLAN_ACTION}, MaxActionsPerRound: 32, Bindings: menuBindings(entry.bindings)}
		if refusal != "" {
			option.DisabledReason = refusal
		}
		if s.WritesFrozen {
			option.DisabledReason = "mutation_owner_not_quiescent"
		}
		if menu.RemainingBatches == 0 {
			option.DisabledReason = "incident_budget_exhausted"
		}
		menu.Options = append(menu.Options, option)
	}
	if len(menu.Options) > 0 {
		menu.Bindings = menu.Options[0].Bindings
		for _, option := range menu.Options[1:] {
			if !sameSelectionBindings(option.Bindings, menu.Bindings) {
				menu.Bindings = nil
				break
			}
		}
	}
	if menu.RoundLimit == 0 {
		menu.RoundLimit = 1
	}
	menu.MenuId = "menu-" + menuDigest(menu)
	l.menu = menu
	l.menuEntries = entries
	response.Menu = proto.Clone(menu).(*contracts.MitigationMenu)
	response.Summary = fmt.Sprintf("%d mitigation options", len(menu.Options))
	if refusal != "" {
		response.Summary += "; refused: " + refusal
	}
}
func (l *ResponderLoop) mitigateMenu(ctx context.Context, selection *contracts.MitigationSelection, response *ToolResponse) {
	if l.selection == nil {
		response.Error = "open a mitigation menu first"
		return
	}
	attempt, retry, err := l.selection.accept(ctx, l.menu, selection)
	if err != nil {
		response.Error = "mitigation selection refused"
		return
	}
	if retry {
		response.Report = cloneSelectionReport(attempt.Report)
		response.Summary = "selection pending"
		if attempt.Report != nil {
			response.Summary = "immutable selection report"
		}
		return
	}
	report := &contracts.MitigationReport{Version: 1, IncidentId: selection.IncidentId, SessionId: selection.SessionId, RequestId: selection.RequestId, AttemptId: attempt.AttemptId, AcceptanceEpoch: attempt.AcceptanceEpoch, Status: contracts.MitigationReport_STATUS_UNRESOLVED}
	used := uint32(0)
	err = l.mutationOwner.run(ctx, selection.DispatchEpoch, func(callCtx context.Context) error {
		return l.executeMenu(callCtx, selection, attempt, report, &used)
	})
	if err != nil {
		report.Status = contracts.MitigationReport_STATUS_INTERRUPTED
		report.FailedPhase = "forward"
		report.ReasonCode = "mutation_owner_interrupted"
		if errors.Is(err, context.DeadlineExceeded) {
			report.Status = contracts.MitigationReport_STATUS_TIMEOUT
		}
	}
	report.WritesFrozen = l.selection.snapshot().WritesFrozen
	settleCtx, cancelSettlement := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelSettlement()
	settled, err := l.selection.settle(settleCtx, attempt.AttemptId, report, used)
	if err != nil {
		response.Error = "mitigation settlement unavailable; writes prohibited"
		return
	}
	response.Report = settled
	response.Summary = "mitigation " + settled.Status.String()
	s := l.selection.snapshot()
	l.batches = int(s.UsedBatches + s.ReservedBatches)
}
func (l *ResponderLoop) menuFence(ctx context.Context) error {
	if !l.MenuConfig.valid() || (l.menu != nil && (l.menu.PolicyDigest != l.MenuConfig.Safety.Digest || l.menu.ScopeDigest != menuDigest(l.MenuConfig.Dependencies))) {
		return errSelectionFenced
	}
	if l.CheckHealth != nil {
		if err := l.CheckHealth(ctx); err != nil {
			return err
		}
	}
	if l.Executor.AcceptedRevision != nil && l.Executor.AcceptedRevision() != l.Request.SourceCommit {
		return errSelectionFenced
	}
	return l.selection.authorizeMutation(ctx, l.selection.snapshot().DispatchEpoch)
}
func (l *ResponderLoop) menuBarrier() *CausalBarrier {
	return &CausalBarrier{Journal: l.selection, Reader: menuSafeReader{l.MenuConfig, l.Request.Namespace}, Namespace: l.Request.Namespace, Dependencies: l.MenuConfig.Dependencies, AuthorizeRead: l.MenuConfig.Safety.AuthorizeRead, CheckFence: l.menuFence}
}
func (l *ResponderLoop) executeMenu(ctx context.Context, selection *contracts.MitigationSelection, attempt *contracts.SelectionAttempt, report *contracts.MitigationReport, used *uint32) error {
	fail := func(status contracts.MitigationReport_Status, phase, reason string) {
		report.Status = status
		report.FailedPhase = phase
		report.ReasonCode = reason
	}
	if !l.MenuConfig.valid() {
		fail(contracts.MitigationReport_STATUS_REFUSED, "safety", "safety_capability_unavailable")
		return nil
	}
	if selection.PolicyDigest != l.MenuConfig.Safety.Digest || selection.ScopeDigest != menuDigest(l.MenuConfig.Dependencies) || selection.SourceCommit != l.Request.SourceCommit {
		fail(contracts.MitigationReport_STATUS_SUPERSEDED, "acceptance", "menu_scope_or_policy_changed")
		return nil
	}
	entry, ok := l.menuEntries[selection.OptionId]
	if !ok {
		fail(contracts.MitigationReport_STATUS_REFUSED, "acceptance", "option_unavailable")
		return nil
	}
	if _, err := l.menuSnapshot(ctx); err != nil {
		fail(contracts.MitigationReport_STATUS_REFUSED, "safety", "unsafe_consumer_or_content")
		return nil
	}
	barrier := l.menuBarrier()
	var applications []PlanApplicationResult
	var clients []*menuPlanClient
	for round := uint32(1); round <= selection.RoundLimit; round++ {
		roundCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		observation, err := barrier.Capture(roundCtx, attempt.AttemptId, round, "before_round", nil)
		if err != nil {
			cancel()
			fail(contracts.MitigationReport_STATUS_UNRESOLVED, "barrier", "fresh_observation_unavailable")
			break
		}
		// A completed fresh observation starts a round, even when preflight refuses.
		*used = round
		rr := &contracts.MitigationRoundReport{RoundIndex: round, RoundId: fmt.Sprintf("%s-round-%d", attempt.AttemptId, round), BarrierIds: []string{observation.Receipt.CaptureId}}
		report.Rounds = append(report.Rounds, rr)
		view := menuReadView{DetectionContext: observation.View}
		var diagnoser program.Diagnoser
		if l.Programs != nil {
			diagnoser, _ = l.Programs.Diagnoser(entry.programID)
		}
		if diagnoser == nil {
			cancel()
			fail(contracts.MitigationReport_STATUS_REFUSED, "diagnosis", "bound_diagnoser_unavailable")
			break
		}
		verdict := program.RunDiagnose(roundCtx, diagnoser, view, entry.bindings, l.deadline())
		rr.Diagnosis = verdictContract(program.Verdict{Result: verdict.Result})
		if verdict.Result != program.Match {
			cancel()
			fail(contracts.MitigationReport_STATUS_DIFFERENT_CAUSE, "diagnosis", "bound_cause_no_longer_matches")
			break
		}
		plan := program.Plan{}
		if entry.plan != nil {
			plan = *entry.plan
		} else {
			mitigator, _ := l.Programs.Mitigator(entry.programID)
			if mitigator == nil {
				cancel()
				fail(contracts.MitigationReport_STATUS_REFUSED, "planning", "mitigator_unavailable")
				break
			}
			l.mu.Unlock()
			plan, err = runBoundedMitigator(roundCtx, mitigator, view, entry.bindings, l.deadline())
			l.mu.Lock()
		}
		if err != nil || plan.Validate() != nil || len(plan.Actions) == 0 || len(plan.Actions) > 32 {
			cancel()
			fail(contracts.MitigationReport_STATUS_REFUSED, "planning", "invalid_or_empty_plan")
			break
		}
		client := &menuPlanClient{PlanObjectClient: l.Executor.Client, loop: l, attempt: attempt.AttemptId, round: round, phase: "forward", receipts: map[string]string{}}
		executor := l.Executor
		executor.Client = client
		if executor.Source != nil {
			executor.Source = menuPlanSource{PlanSource: executor.Source, client: client}
		}
		// Existing rollout waiting remains in use. Menu verification below uses the
		// direct barrier rather than the executor's potentially stale cache/verifier.
		executor.Observer = nil
		executor.Verifier = menuDeferredVerifier{}
		preview, previewErr := executor.Apply(roundCtx, l.Request, plan, true)
		if previewErr == nil {
			// Refuse the complete plan before its first write if any effect lacks
			// evidence. Each real write and compensation still rechecks the policy.
			for _, patch := range preview.Patches {
				if l.MenuConfig.Safety.AuthorizeEffect(roundCtx, patch.Ref, patch.Before, patch.After) != nil {
					previewErr = errMenuSafety
					break
				}
			}
		}
		if previewErr != nil {
			cancel()
			fail(contracts.MitigationReport_STATUS_REFUSED, "planning", "plan_preflight_refused")
			break
		}
		var absent []EffectExpectation
		for _, patch := range preview.Patches {
			if patch.Operation == "create" {
				absent = append(absent, EffectExpectation{Ref: patch.Ref, RequireAbsent: true})
			}
		}
		priorAbsence := ""
		if len(absent) > 0 {
			before, err := barrier.Capture(roundCtx, attempt.AttemptId, round, "before_round", absent)
			if err != nil {
				cancel()
				fail(contracts.MitigationReport_STATUS_UNRESOLVED, "barrier", "creation_absence_unproven")
				break
			}
			priorAbsence = before.Receipt.CaptureId
			rr.BarrierIds = append(rr.BarrierIds, priorAbsence)
		}
		result, applyErr := executor.Apply(roundCtx, l.Request, plan, false)
		applications = append(applications, result)
		clients = append(clients, client)
		l.menuRoundReceipts(attempt.AttemptId, round, rr)
		if applyErr != nil {
			cancel()
			fail(contracts.MitigationReport_STATUS_EXECUTION_ERROR, "forward", "plan_execution_failed")
			break
		}
		effects := menuEffectExpectations(result, priorAbsence)
		after, err := barrier.Capture(roundCtx, attempt.AttemptId, round, "after_write", effects)
		if err != nil {
			cancel()
			fail(contracts.MitigationReport_STATUS_UNRESOLVED, "barrier", "effect_not_observed")
			break
		}
		rr.BarrierIds = append(rr.BarrierIds, after.Receipt.CaptureId)
		verified := program.RunDiagnose(roundCtx, diagnoser, menuReadView{DetectionContext: after.View}, entry.bindings, l.deadline())
		rr.Verification = verdictContract(program.Verdict{Result: verified.Result})
		healthy, healthErr := l.MenuConfig.Healthy(roundCtx, after.View)
		rr.IndependentlyHealthy = healthErr == nil && healthy
		rr.OriginalFindingsClear = l.menuFindingsClear(roundCtx, after.View)
		if verified.Result == program.NoMatch && rr.IndependentlyHealthy && rr.OriginalFindingsClear {
			closure, closureErr := barrier.Capture(roundCtx, attempt.AttemptId, round, "closure", effects)
			if closureErr == nil {
				final := program.RunDiagnose(roundCtx, diagnoser, menuReadView{DetectionContext: closure.View}, entry.bindings, l.deadline())
				finalHealthy, finalErr := l.MenuConfig.Healthy(roundCtx, closure.View)
				if final.Result == program.NoMatch && finalErr == nil && finalHealthy && l.menuFindingsClear(roundCtx, closure.View) {
					rr.BarrierIds = append(rr.BarrierIds, closure.Receipt.CaptureId)
					report.Status = contracts.MitigationReport_STATUS_SUCCESS
					l.findings = nil
					cancel()
					break
				}
			}
			cancel()
			fail(contracts.MitigationReport_STATUS_UNRESOLVED, "closure", "closure_not_verified")
			break
		}
		cancel()
		if verified.Result == program.NoMatch {
			fail(contracts.MitigationReport_STATUS_DIFFERENT_CAUSE, "verification", "independent_health_unresolved")
			break
		}
		if verified.Result != program.Match || healthErr != nil {
			fail(contracts.MitigationReport_STATUS_UNRESOLVED, "verification", "verification_unknown")
			break
		}
		fail(contracts.MitigationReport_STATUS_UNRESOLVED, "verification", "round_limit_reached")
	}
	settlingCtx, cancelSettling := context.WithTimeout(ctx, 30*time.Second)
	defer cancelSettling()
	// Ordinary failure uses the existing guarded rollback, with the predicate
	// evaluated again at every compensation patch. Different cause retains repairs.
	if report.Status != contracts.MitigationReport_STATUS_SUCCESS && report.Status != contracts.MitigationReport_STATUS_DIFFERENT_CAUSE {
		for i := len(applications) - 1; i >= 0; i-- {
			result := applications[i]
			client := clients[i]
			client.phase = "compensation"
			saved, _ := selectionAttempt(l.selection.snapshot(), attempt.AttemptId)
			for k := len(saved.Actions) - 1; k >= 0; k-- {
				action := saved.Actions[k]
				if action.RoundIndex == client.round && action.Phase == "forward" && action.Complete {
					client.compensation = append(client.compensation, action.Receipt.ActionId)
				}
			}
			executor := l.Executor
			executor.Client = client
			if executor.Source != nil {
				executor.Source = menuPlanSource{PlanSource: executor.Source, client: client}
			}
			if result.Applied && !result.RolledBack {
				if _, err := l.menuSnapshot(settlingCtx); err != nil {
					report.FailedPhase = "compensation"
					report.ReasonCode = "compensation_safety_refused"
					for _, patch := range result.Patches {
						report.Residuals = append(report.Residuals, planRefProto(patch.Ref))
					}
					continue
				}
				if err := executor.Rollback(settlingCtx, result); err != nil {
					report.FailedPhase = "compensation"
					report.ReasonCode = "compensation_failed"
					for _, patch := range result.Patches {
						report.Residuals = append(report.Residuals, planRefProto(patch.Ref))
					}
				}
				if _, err := barrier.Capture(settlingCtx, attempt.AttemptId, client.round, "after_compensation", nil); err != nil {
					report.FailedPhase = "compensation"
					report.ReasonCode = "compensation_observation_unavailable"
					for _, patch := range result.Patches {
						report.Residuals = append(report.Residuals, planRefProto(patch.Ref))
					}
				}
			}
		}
	}
	state := l.selection.snapshot()
	saved, _ := selectionAttempt(state, attempt.AttemptId)
	report.Barriers = saved.Barriers
	for _, rr := range report.Rounds {
		l.menuRoundReceipts(attempt.AttemptId, rr.RoundIndex, rr)
	}
	return l.selection.recordRounds(ctx, attempt.AttemptId, report.Rounds)
}
func (l *ResponderLoop) menuFindingsClear(ctx context.Context, view sdk.DetectionContext) (clear bool) {
	defer func() {
		if recover() != nil {
			clear = false
		}
	}()
	covered := map[string]bool{}
	for _, detector := range l.Detectors {
		covered[detector.Spec().ID] = true
		findings, err := detector.Detect(ctx, view)
		if err != nil || len(findings) > 0 {
			return false
		}
	}
	for _, finding := range l.Request.Findings {
		if finding.DetectorID != "" && !strings.HasPrefix(finding.DetectorID, "drift.") && !covered[finding.DetectorID] {
			return false
		}
	}
	findings, _, _ := checkRetainedDrift(ctx, l.Request.Findings, menuReadView{DetectionContext: view}, l.Programs, l.deadline())
	return len(findings) == 0
}
func (l *ResponderLoop) menuRoundReceipts(attempt string, round uint32, rr *contracts.MitigationRoundReport) {
	rr.Actions = nil
	state := l.selection.snapshot()
	a, _ := selectionAttempt(state, attempt)
	for _, action := range a.Actions {
		if action.RoundIndex != round {
			continue
		}
		if action.Phase == "forward" {
			rr.Actions = append(rr.Actions, proto.Clone(action.Receipt).(*contracts.MitigationActionReceipt))
		} else {
			for _, r := range rr.Actions {
				if r.ActionId == action.Receipt.ActionId {
					r.CompensationState = action.Receipt.State
				}
			}
		}
	}
}
func (j *selectionJournal) recordRounds(ctx context.Context, id string, rounds []*contracts.MitigationRoundReport) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	next := j.next()
	a, err := selectionAttempt(next.SelectionLedger, id)
	if err != nil {
		return err
	}
	if a.Report != nil {
		return errSelectionFenced
	}
	a.Rounds = nil
	for _, r := range rounds {
		a.Rounds = append(a.Rounds, proto.Clone(r).(*contracts.MitigationRoundReport))
	}
	return j.save(ctx, next)
}

type menuDeferredVerifier struct{}

func (menuDeferredVerifier) Verify(context.Context) (bool, error) { return false, nil }

// No opaque live logs, probes or textual source diffs are granted to menu
// programs. Producers need an explicit provenance capability before expansion.
type menuReadView struct{ sdk.DetectionContext }

func (menuReadView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return "", errMenuSafety
}
func (menuReadView) ProbeTCP(sdk.ObjectRef, int32) (program.ProbeResult, error) {
	return program.ProbeResult{}, errMenuSafety
}
func (menuReadView) ProbeHTTP(sdk.ObjectRef, program.HTTPProbe) (program.ProbeResult, error) {
	return program.ProbeResult{}, errMenuSafety
}
func (menuReadView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, errMenuSafety
}
func (menuReadView) DiffAgainstBaseline(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, errMenuSafety
}

func menuEffectExpectations(result PlanApplicationResult, priorAbsence string) []EffectExpectation {
	var effects []EffectExpectation
	byRef := map[sdk.ObjectRef]int{}
	for _, patch := range result.Patches {
		effect := EffectExpectation{Ref: patch.Ref, Before: patch.Before, After: patch.After}
		switch patch.Operation {
		case "create":
			effect.Before = nil
			effect.PriorAbsenceCaptureID = priorAbsence
		case "delete":
			effect.RequireAbsent = true
		default:
			if patch.Before != nil {
				effect.UID = string(patch.Before.GetUID())
			}
		}
		if index, exists := byRef[patch.Ref]; exists {
			effects[index].After = effect.After
		} else {
			byRef[patch.Ref] = len(effects)
			effects = append(effects, effect)
		}
	}
	return effects
}
