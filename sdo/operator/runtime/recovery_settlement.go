package runtime

import (
	"context"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// SettleRecoveryApplications refreshes accepted stage evidence after the
// controller's independent recovery proof. It observes only: no apply, retry,
// compensation or publication authority is recreated here.
func (l *ResponderLoop) SettleRecoveryApplications(ctx context.Context, result IncidentResult, stages []*contracts.StageReport) (IncidentResult, []*contracts.StageReport) {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.settleRecoveryApplicationsLocked(ctx, result, stages)
}

func (l *ResponderLoop) settleRecoveryApplicationsLocked(ctx context.Context, result IncidentResult, stages []*contracts.StageReport) (IncidentResult, []*contracts.StageReport) {
	stages = cloneStageReports(stages)
	if l.Programs == nil || l.Executor.Client == nil || l.Executor.Rollouts == nil {
		return result, stages
	}
	view, err := l.snapshotView(ctx)
	if err != nil {
		return result, stages
	}
	executor := l.Executor
	executor.PollRollouts = true
	// The proof is independent of an expired response budget; observation remains
	// bounded by the controller context and the executor's single-sample bound.
	request := l.Request
	request.ResponseDeadline = time.Time{}
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	getFinal := func(ref sdk.ObjectRef) *unstructured.Unstructured {
		if object, ok := objects[ref]; ok {
			return object
		}
		object, err := executor.Client.Get(ctx, ref)
		if err != nil {
			object = nil
		}
		objects[ref] = object
		return object
	}
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			id := attempt.GetApplyReceipt().GetApplicationId()
			for index := range l.applicationsLog {
				app := &l.applicationsLog[index]
				if app.ID != id || !app.Result.Applied || app.Result.DryRun || app.RolledBack || app.Result.RolledBack || app.RollbackError != "" || hasAppliedScript(app.Result.Plan.Actions) {
					continue
				}
				// Execution errors are history, not a transient observation. Only pending
				// rollout/readback or measured source residuals can be superseded by proof.
				if app.Error != "" && app.Result.RolloutPending == nil && !strings.HasPrefix(app.Error, "source_reconcile_partial:") && !strings.HasPrefix(app.Error, "rollout_stalled:") {
					continue
				}
				if pending, ok := l.pendingWrites[id]; ok && pending.Superseded {
					continue
				}
				if attempt.ProgramRevision == "" || attempt.ProgramRevision != l.programRevision(attempt.PlaybookId) {
					continue
				}
				diagnoser, ok := l.Programs.Diagnoser(attempt.PlaybookId)
				if !ok {
					continue
				}
				refs, err := l.menuPlanWriteRefs(app.Result.Plan)
				if err != nil {
					continue
				}
				pending, rolloutErr := executor.waitForRollouts(ctx, request, refs)
				app.Result.VerificationCompleted = true
				app.Result.VerificationPassed = false
				app.Result.RolloutPending = pending
				sourceErr := executor.reportSourceResiduals(ctx, request, &app.Result)
				verdict := program.RunDiagnose(ctx, diagnoser, view, programBindingsState(attempt.Bindings), l.deadline())
				run := ProgramRun{ProgramID: attempt.PlaybookId, ProgramRevision: attempt.ProgramRevision, Bindings: programBindingsState(attempt.Bindings), Verdict: verdict}
				l.runs = append(l.runs, run)
				attempt.PostApplyDiagnose = programRunContract(run)
				exact := finalPlanFieldsMatch(app.Result, getFinal)
				app.Result.FinalEquivalenceVerified = &exact
				if rolloutErr == nil && sourceErr == nil && verdict.Result == program.NoMatch && exact {
					app.Result.VerificationPassed = true
					app.Result.RolloutPending = nil
					app.Error = ""
					app.Pending = false
					attempt.Error = ""
					attempt.Result = "applied_cleared"
					attempt.RemainingFindingFingerprints = nil
					delete(l.pendingWrites, id)
				}
				l.applications[id] = app.Result
				receipt, err := recordedPlanContract(*app)
				if err == nil {
					attempt.ApplyReceipt = receipt
				}
			}
		}
	}
	// Replace evidence by application identity; preserve manual repairs, sessions
	// and resolution metadata. Historical stage snapshots no longer override it.
	for i := range result.PlanApplications {
		for _, app := range l.applicationsLog {
			if result.PlanApplications[i].ID == app.ID {
				result.PlanApplications[i] = app
			}
		}
	}
	result.ProgramRuns = append([]ProgramRun(nil), l.runs...)
	existing := []RepairActionReceipt{}
	for _, receipt := range result.RepairActions {
		generated := false
		for _, app := range result.PlanApplications {
			base := "sdo-program-" + app.ID
			generated = generated || receipt.ActionID == base || strings.HasPrefix(receipt.ActionID, base+"-")
		}
		if !generated {
			existing = append(existing, receipt)
		}
	}
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, existing)
	return result, stages
}
