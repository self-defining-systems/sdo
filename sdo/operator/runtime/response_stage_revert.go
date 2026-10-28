package runtime

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// revertIneffective never hands the responder a half-fixed system. An attempt
// of this run that applied, cleared none of its findings and whose program
// still diagnoses the same fault on a fresh observation made no progress; its
// writes are restored from the executor's preimages. An attempt that cleared
// a finding or its own diagnosis is kept (partial), and so is one with an
// effect a preimage cannot undo (irreversible). The rollback goes through the
// ordinary tool boundary, so its receipt and fences are the responder's own.
func (stage *DeterministicResponseStage) revertIneffective(ctx context.Context, loop *ResponderLoop, state *IncidentState, report *contracts.StageReport, attempts []*contracts.StageMitigation, save func(bool) error) error {
	var candidates []*contracts.StageMitigation
	var selected []selectedPlan
	for _, attempt := range attempts {
		if attempt.GetResult() != "applied_still_matching" {
			continue
		}
		if len(attempt.GetClearedFindingFingerprints()) > 0 {
			attempt.KeptReason = "partial"
			continue
		}
		application, ok := loop.applications[attempt.GetApplyReceipt().GetApplicationId()]
		if !ok || !revertRestoresPreimage(application) || sharesApplication(report, attempt) {
			attempt.KeptReason = "irreversible"
			continue
		}
		candidates = append(candidates, attempt)
		selected = append(selected, selectedPlan{programID: attempt.PlaybookId, revision: attempt.ProgramRevision, bindings: programBindingsState(attempt.Bindings)})
	}
	if len(candidates) == 0 {
		return save(false)
	}
	// Later attempts may have changed what an earlier one diagnoses, so the
	// decision uses a fresh observation, not the post-apply one.
	observed := ToolResponse{}
	loop.verify(ctx, selected, &observed, true)
	updateStageFindings(state, observed)
	open := map[string]bool{}
	for _, finding := range state.Findings {
		open[FindingFingerprint(finding)] = true
	}
	for _, attempt := range candidates {
		matching := false
		for _, run := range observed.ProgramRuns {
			if run.ProgramID == attempt.PlaybookId && run.ProgramRevision == attempt.ProgramRevision && run.Bindings.Equal(programBindingsState(attempt.Bindings)) {
				matching = run.Verdict.Result == program.Match
			}
		}
		for _, fp := range attempt.GetRemainingFindingFingerprints() {
			matching = matching && open[fp]
		}
		if !matching {
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
			return false
		}
	}
	return true
}

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
}
