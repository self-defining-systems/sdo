package runtime

import (
	"context"
	"slices"
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
	initialRuns := len(l.runs)
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
	applications := l.applicationEvidenceLocked()
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			id := attempt.GetApplyReceipt().GetApplicationId()
			for index := range applications {
				app := &applications[index]
				if app.ID != id || !app.Result.Applied || app.Result.DryRun || app.RolledBack || app.Result.RolledBack || app.RollbackError != "" || app.Result.RollbackError != "" || len(app.Result.ActionsApplied) != len(app.Result.Plan.Actions) || hasAppliedScript(app.Result.Plan.Actions) {
					continue
				}
				// Execution errors are history, not a transient observation. Only pending
				// rollout/readback or measured source residuals can be superseded by proof.
				pendingWrite, wasPending := l.pendingWrites[id]
				if pendingWrite.Superseded || recoverySuperseded(*app, result, applications) || !recoveryObservationOnly(*app, wasPending) {
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
				switch {
				case rolloutErr != nil:
					app.Error = rolloutErr.Error()
					attempt.Result = "rollout_failed"
					if pending != nil {
						attempt.Result = "rollout_pending_at_handoff"
					}
				case sourceErr != nil:
					app.Error = sourceErr.Error()
					attempt.Result = "rollout_failed"
				case verdict.Result != program.NoMatch:
					attempt.Result = "applied_still_matching"
				case !exact:
					attempt.Result = "rollout_failed"
				}
				attempt.Error = app.Error
				if rolloutErr == nil && sourceErr == nil && verdict.Result == program.NoMatch && exact {
					app.Result.VerificationPassed = true
                    app.Result.FinalVerificationBasis = "recovery-settled"
					app.Result.RolloutPending = nil
					app.Error = ""
					app.Pending = false
					attempt.Error = ""
					attempt.Result = "applied_cleared"
					attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, attempt.RemainingFindingFingerprints...)
					stage.ClearedFindingFingerprints = append(stage.ClearedFindingFingerprints, attempt.ClearedFindingFingerprints...)
					stage.RemainingFindingFingerprints = slices.DeleteFunc(stage.RemainingFindingFingerprints, func(fp string) bool { return slices.Contains(attempt.ClearedFindingFingerprints, fp) })
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

	stageResult, _ := responsePipelineResult(&IncidentState{Request: l.Request, PriorStages: stages}, result.Timing.StartedAt, result.Timing.CompletedAt)
	settled := &recoveryApplicationSettlement{causes: stageResult.ConfirmedRootCauses, stages: stages, applications: applications, runs: append([]ProgramRun(nil), l.runs[initialRuns:]...)}
	// Update observation facts only. Restored receipts never become prepared
	// plans or rollback handles. The final closure persists this projection.
	l.archivedApplications = applications
	for i := range l.applicationsLog {
		for _, app := range applications {
			if app.ID == l.applicationsLog[i].ID {
				l.applicationsLog[i] = app
			}
		}
	}
	return projectRecoverySettlement(result, settled), stages
}

type recoveryApplicationSettlement struct {
	causes       []ConfirmedRootCause
	stages       []*contracts.StageReport
	applications []RecordedPlanApplication
	runs         []ProgramRun
}

// Replace evidence by application identity; preserve manual repairs, sessions
// and resolution metadata. Historical stage snapshots no longer override it.
func projectRecoverySettlement(result IncidentResult, settled *recoveryApplicationSettlement) IncidentResult {
	generatedIDs := map[string]bool{}
	for _, receipt := range ProgramRepairReceipts(result.PlanApplications, nil) {
		generatedIDs[receipt.ActionID] = true
	}
	for i := range result.PlanApplications {
		for _, app := range settled.applications {
			if result.PlanApplications[i].ID == app.ID {
				result.PlanApplications[i] = app
			}
		}
	}
	result.ProgramRuns = append(result.ProgramRuns, settled.runs...)
	result.ConfirmedRootCauses = reconcileEarlyCauses(settled.causes, result.ConfirmedRootCauses)
	existing := []RepairActionReceipt{}
	for _, receipt := range result.RepairActions {
		if !generatedIDs[receipt.ActionID] {
			existing = append(existing, receipt)
		}
	}
	result.RepairActions = ProgramRepairReceipts(result.PlanApplications, existing)
	return result
}

// Typed action/readback evidence distinguishes an unsettled observation from
// an execution failure. Error text is presentation and grants no credit.
func recoveryObservationOnly(app RecordedPlanApplication, wasPending bool) bool {
	if len(app.Result.ActionReports) != len(app.Result.Plan.Actions) {
		return false
	}
	observation := wasPending || app.Result.RolloutPending != nil
	for _, report := range app.Result.ActionReports {
		sourcePartial := report.SourceReconcile != nil && !report.SourceReconcile.Converged
		if report.RecordingError != "" || (report.FailureReason != "" && !(sourcePartial && report.FailureReason == "source_residual")) {
			return false
		}
		if report.Status != "applied" && !(sourcePartial && report.Status == "partial") {
			return false
		}
		observation = observation || sourcePartial
	}
	return observation || app.Error == ""
}

// Reject overlapping later corrective effects, not independent causes or
// diagnostic helper writes. Unknown ordering is deliberately not positive proof.
func recoverySuperseded(app RecordedPlanApplication, result IncidentResult, applications []RecordedPlanApplication) bool {
	if app.StartedAt.IsZero() || app.CompletedAt.IsZero() {
		return true
	}
	generated := map[string]bool{}
	for _, receipt := range ProgramRepairReceipts(applications, nil) {
		generated[receipt.ActionID] = true
	}
	own := ProgramRepairReceipts([]RecordedPlanApplication{app}, nil)
	overlaps := func(refs []sdk.ObjectRef) bool {
		for _, receipt := range own {
			for _, target := range receipt.Resources {
				for _, ref := range refs {
					if ref.Kind == "*" || sameResource(target, ref) {
						return true
					}
				}
			}
		}
		return false
	}
	for _, other := range applications {
		if other.ID == app.ID || (!other.Result.Applied && len(other.Result.ActionsApplied) == 0) {
			continue
		}
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{other}, nil) {
			if overlaps(receipt.Resources) && (other.StartedAt.IsZero() || !other.StartedAt.Before(app.StartedAt)) {
				return true
			}
		}
	}
	for _, receipt := range result.RepairActions {
		if generated[receipt.ActionID] {
			continue
		}
		if overlaps(receipt.Resources) && (receipt.StartedAt.IsZero() || !receipt.StartedAt.Before(app.StartedAt)) {
			return true
		}
	}
	return false
}
