package runtime

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
<<<<<<< HEAD
	"sdo.dev/operator/sdk/program"
)

// revertIneffective never hands the agent a half-fixed system. Progress is
// judged on a fresh observation after every attempt has run, not at apply
// time: a finding that cleared only until the replacement Pod failed again is
// not progress. An applied attempt of this run whose target findings are all
// still open and whose program still diagnoses the same fault made none; its
// writes are restored from the executor's preimages. An attempt that cleared a
// finding or its own diagnosis is kept (partial), and so is one with an effect
// a preimage cannot undo (irreversible) or whose restore failed.
func (s *DeterministicStage) revertIneffective(ctx context.Context, loop *ResponderLoop, state *IncidentState, attempts []*contracts.StageMitigation) {
=======
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// revertIneffective never hands the responder a half-fixed system. Progress is
// judged at handoff on a fresh observation, not at apply time: a finding that
// cleared only until the replacement Pod failed again is not progress. An
// applied attempt of this run whose target findings are all open and whose
// program still diagnoses the same fault made none; its writes are restored
// from the executor's preimages. An attempt that cleared a finding or its own
// diagnosis is kept (partial), and so is one with an effect a preimage cannot
// undo (irreversible). The rollback goes through the ordinary tool boundary,
// so its receipt and fences are the responder's own.
func (stage *DeterministicResponseStage) revertIneffective(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, attempts []*contracts.StageMitigation, save func(bool) error) error {
>>>>>>> origin/vic-lsh/dev/sdo-improve
	var candidates []*contracts.StageMitigation
	var selected []selectedPlan
	for _, attempt := range attempts {
		if attempt.GetResult() == "applied_still_matching" {
			candidates = append(candidates, attempt)
<<<<<<< HEAD
			selected = append(selected, selectedPlan{programID: attempt.PlaybookId, bindings: stageBindingsState(attempt.Bindings)})
		}
	}
	if len(candidates) == 0 {
		return
	}
	observed := ToolResponse{}
	loop.locked(func() { loop.verify(ctx, selected, &observed) })
	if observed.Error != "" {
		// Without a completed observation nothing is judged ineffective.
		return
	}
	state.Findings, state.DetectorStates, state.Healthy = observed.Findings, observed.DetectorStates, observed.Healthy
=======
			selected = append(selected, selectedPlan{programID: attempt.PlaybookId, revision: attempt.ProgramRevision, bindings: programBindingsState(attempt.Bindings)})
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	observed := ToolResponse{}
	loop.verify(ctx, selected, &observed, true)
	updateStageFindings(state, observed)
>>>>>>> origin/vic-lsh/dev/sdo-improve
	open := map[string]bool{}
	for _, finding := range state.Findings {
		open[FindingFingerprint(finding)] = true
	}
	for _, attempt := range candidates {
<<<<<<< HEAD
		cleared := false
		for _, fingerprint := range attempt.TargetFindingFingerprints {
			cleared = cleared || !open[fingerprint]
		}
		bindings := stageBindingsState(attempt.Bindings)
		matching := false
		for _, run := range observed.ProgramRuns {
			if run.ProgramID == attempt.PlaybookId && bindingKey(run.Bindings) == bindingKey(bindings) {
				matching = run.Verdict.Result == program.Match
			}
		}
		id := attempt.GetApplyReceipt().GetApplicationId()
		var restorable bool
		loop.locked(func() {
			application, known := loop.applications[id]
			restorable = known && revertRestoresPreimage(application)
		})
		switch {
		case cleared || !matching:
			attempt.KeptReason = "partial"
			continue
		case !restorable:
			attempt.KeptReason = "irreversible"
			continue
		}
		if err := loop.stageRollback(ctx, id); err != nil {
			attempt.KeptReason, attempt.Error = "rollback_failed", err.Error()
			continue
		}
		attempt.Result = "reverted_ineffective"
		if receipt := loop.applicationReceipt(id); receipt != nil {
			attempt.ApplyReceipt = receipt
		}
		reobserveStage(ctx, loop, state)
	}
}

// revertRestoresPreimage reports whether a rollback returns every object the
// application wrote to its state before it. A restart has nothing to restore:
// the Pods already restarted, and a second restart is not an undo.
func revertRestoresPreimage(application PlanApplicationResult) bool {
	if !application.Applied || application.RolledBack || application.PendingAction != nil || len(application.ActionsApplied) > len(application.Patches) {
		return false
	}
	for index, action := range application.ActionsApplied {
		switch action.(type) {
		case program.RestartRollout, *program.RestartRollout:
			return false
		}
		if patch := application.Patches[index]; patch.Before == nil || patch.After == nil {
=======
		targets := append(append([]string(nil), attempt.GetClearedFindingFingerprints()...), attempt.GetRemainingFindingFingerprints()...)
		cleared := false
		for _, fp := range targets {
			cleared = cleared || !open[fp]
		}
		// A deleted or replaced binding no longer diagnoses at all, so the
		// diagnosis counts as progress only for an effect that can be undone.
		matching := false
		for _, run := range observed.ProgramRuns {
			if run.ProgramID == attempt.PlaybookId && run.ProgramRevision == attempt.ProgramRevision && run.Bindings.Equal(programBindingsState(attempt.Bindings)) {
				matching = run.Verdict.Result == program.Match
			}
		}
		application, known := loop.applications[attempt.GetApplyReceipt().GetApplicationId()]
		switch {
		case cleared:
			attempt.KeptReason = "partial"
			continue
		case !known || !revertRestoresPreimage(application) || sharesApplication(report, attempt):
			attempt.KeptReason = "irreversible"
			continue
		case !matching:
			attempt.KeptReason = "partial"
			continue
		}
		var reverted ToolResponse
		if err := withoutResponderLocks(loop, "revert-ineffective", true, func() {
			reverted = loop.Handle(ctx, ToolRequest{Command: "rollback", ApplicationID: attempt.ApplyReceipt.ApplicationId, stage: true})
		}); err != nil {
			return err
		}
		for _, application := range loop.applicationsLog {
			if application.ID != attempt.ApplyReceipt.ApplicationId {
				continue
			}
			receipt, err := recordedPlanContract(application)
			if err != nil {
				return err
			}
			attempt.ApplyReceipt = receipt
		}
		if !attempt.ApplyReceipt.GetRolledBack() {
			attempt.KeptReason = "rollback_failed"
		} else {
			attempt.Result = stageRevertedIneffective
			updateStageFindings(state, reverted)
			recordStageFindingProgress(attempt, targets, state.Findings)
		}
		if err := save(false); err != nil {
			return err
		}
	}
	return save(false)
}

// revertRestoresPreimage reports whether a rollback returns every object the
// application wrote to its state before it. Deletes, execs, scripts, external
// calls and restarts have no preimage to restore: a second restart is not an
// undo of the first.
func revertRestoresPreimage(application PlanApplicationResult) bool {
	if !application.Applied || application.RolledBack || application.RolloutPending != nil || len(application.ActionsApplied) > len(application.Patches) {
		return false
	}
	for index, action := range application.ActionsApplied {
		switch generalAction(action).(type) {
		case program.RestartRollout, *program.RestartRollout:
			return false
		}
		patch := application.Patches[index]
		if patch.Operation == "delete" || patch.Irreversible || (patch.Operation != "create" && (patch.Before == nil || patch.After == nil)) {
			return false
		}
	}
	for _, effect := range application.ObjectEffects {
		if effect.Irreversible && effect.State != "not_applied" {
>>>>>>> origin/vic-lsh/dev/sdo-improve
			return false
		}
	}
	return true
}

<<<<<<< HEAD
// stageRollback restores one recorded application through the executor and
// records the result on the application log.
func (l *ResponderLoop) stageRollback(ctx context.Context, applicationID string) error {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	result := l.applications[applicationID]
	err := l.Executor.rollback(ctx, &result)
	if err != nil {
		result.RollbackError = err.Error()
		l.addFailureSignal("rollback_failure:" + applicationID)
	}
	l.applications[applicationID] = result
	for i := range l.applicationsLog {
		if l.applicationsLog[i].ID == applicationID {
			l.applicationsLog[i].Result = result
			l.applicationsLog[i].RolledBack, l.applicationsLog[i].RollbackError = result.RolledBack, result.RollbackError
		}
	}
	if perr := l.persistToolState(ctx); perr != nil && err == nil {
		return perr
	}
	return err
}

// applicationReceipt projects the latest record of one application.
func (l *ResponderLoop) applicationReceipt(applicationID string) *contracts.PlanApplication {
	var receipt *contracts.PlanApplication
	l.locked(func() {
		for _, application := range l.applicationsLog {
			if application.ID != applicationID {
				continue
			}
			if proto, err := application.Result.Proto(); err == nil {
				proto.ApplicationId, proto.ProgramIds = application.ID, application.ProgramIDs
				proto.RolledBack, proto.RollbackError = application.RolledBack, application.RollbackError
				receipt = proto
			}
		}
	})
	return receipt
=======
// A rollback restores a whole application, so it may not undo another
// attempt's actions.
func sharesApplication(report *contracts.StageReport, attempt *contracts.StageMitigation) bool {
	for _, other := range report.GetMitigations() {
		if other != attempt && other.GetApplyReceipt().GetApplicationId() == attempt.GetApplyReceipt().GetApplicationId() {
			return true
		}
	}
	return false
}

// touchedObjects lists the objects an application's applied actions and
// object effects wrote, once each, in order.
func touchedObjects(result PlanApplicationResult) []*contracts.ObjectRef {
	var refs []*contracts.ObjectRef
	seen := map[sdk.ObjectRef]bool{}
	add := func(ref sdk.ObjectRef) {
		if ref.Name != "" && !seen[ref] {
			seen[ref] = true
			refs = append(refs, planRefProto(ref))
		}
	}
	for _, action := range result.ActionsApplied {
		if ref, err := planActionRef(action); err == nil {
			add(ref)
		}
	}
	for _, effect := range result.ObjectEffects {
		if effect.State != "not_applied" {
			add(effect.Ref)
		}
	}
	return refs
>>>>>>> origin/vic-lsh/dev/sdo-improve
}
