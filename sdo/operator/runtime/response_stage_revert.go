package runtime

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	var candidates []*contracts.StageMitigation
	var selected []selectedPlan
	for _, attempt := range attempts {
		if attempt.GetResult() == "applied_still_matching" {
			candidates = append(candidates, attempt)
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
	open := map[string]bool{}
	for _, finding := range state.Findings {
		open[FindingFingerprint(finding)] = true
	}
	for _, attempt := range candidates {
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
			return false
		}
	}
	return true
}

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
}
