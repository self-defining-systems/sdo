package runtime

import (
	"context"
	"log/slog"
	"sdo.dev/operator/memory"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	stages = l.recoveryStageEvidence(stages)
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
	readErrors := map[sdk.ObjectRef]error{}
	getFinal := func(ref sdk.ObjectRef) *unstructured.Unstructured {
		if object, ok := objects[ref]; ok {
			return object
		}
		object, err := executor.Client.Get(ctx, ref)
		readErrors[ref] = err
		if err != nil {
			object = nil
		}
		objects[ref] = object
		return object
	}
	applications := l.applicationEvidenceLocked()
	observedWrites, historyComplete := l.recoveryAPIWrites(ctx, result, applications)
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			id := attempt.GetApplyReceipt().GetApplicationId()
			for index := range applications {
				app := &applications[index]
				if app.ID != id {
					continue
				}
				// A prior observation is never a permanent grant. Re-establish
				// its exact recovery proof against all later corrective writes.
				app.Result.FinalVerificationBasis = ""
				app.Result.VerificationPassed = false
				app.Result.FinalEquivalenceVerified = nil
				attempt.Result = "applied_still_matching"
				if !app.Result.Applied || app.Result.DryRun || app.RolledBack || app.Result.RolledBack || app.RollbackError != "" || app.Result.RollbackError != "" || len(app.Result.ActionsApplied) != len(app.Result.Plan.Actions) {
					continue
				}
				// Execution errors are history, not a transient observation. Only pending
				// rollout/readback or measured source residuals can be superseded by proof.
				pendingWrite, wasPending := l.pendingWrites[id]
				if pendingWrite.Superseded || !historyComplete || l.recoverySuperseded(*app, result, applications, observedWrites, stage.Stage == "deterministic") || !recoveryObservationOnly(*app) {
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
				if wasPending && len(pendingWrite.Refs) > 0 {
					refs = pendingWrite.Refs
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
				scripted := hasAppliedScript(app.Result.Plan.Actions)
				exact := !scripted && l.recoveryPlanFieldsMatch(app.Result, getFinal, func(ref sdk.ObjectRef) bool { getFinal(ref); return apierrors.IsNotFound(readErrors[ref]) })
				healthy := false
				targets := append(append([]string(nil), attempt.ClearedFindingFingerprints...), attempt.RemainingFindingFingerprints...)
				verifier := scopedMitigationVerifier(executor.Verifier, app.Result.Plan, l.mitigationIncident([]selectedPlan{{programID: attempt.PlaybookId, bindings: programBindingsState(attempt.Bindings), targetFingerprints: targets}}).Findings)
				if verifier != nil {
					healthy, _ = verifier.Verify(ctx)
				} else if executor.Verifier != nil {
					healthy, _ = executor.Verifier.Verify(ctx)
				}
				transition := false
				if scripted && healthy && rolloutErr == nil && sourceErr == nil && verdict.Result == program.NoMatch {
					proof := *app
					proof.Result.VerificationCompleted, proof.Result.VerificationPassed, proof.Error = true, true, ""
					transition = l.finalScriptApplication(ctx, proof, view, selectedPlan{programID: attempt.PlaybookId, revision: attempt.ProgramRevision, bindings: programBindingsState(attempt.Bindings)})
				}
				if !scripted {
					app.Result.FinalEquivalenceVerified = &exact
				}
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
				case !healthy:
					attempt.Result = "applied_still_matching"
				case !exact && !transition:
					attempt.Result = "rollout_failed"
				}
				attempt.Error = app.Error
				if rolloutErr == nil && sourceErr == nil && verdict.Result == program.NoMatch && (exact || transition) && healthy {
					app.Result.VerificationPassed = true
					app.Result.FinalVerificationBasis = "recovery-settled"
					if transition {
						app.Result.FinalVerificationBasis = "observed-transition"
					}
					app.Result.RolloutPending = nil
					app.Error = ""
					app.Pending = false
					attempt.Error = ""
					attempt.Result = "applied_cleared"
					attempt.ClearedFindingFingerprints = append(attempt.ClearedFindingFingerprints, attempt.RemainingFindingFingerprints...)
					stage.ClearedFindingFingerprints = append(stage.ClearedFindingFingerprints, attempt.ClearedFindingFingerprints...)
					slices.Sort(attempt.ClearedFindingFingerprints)
					attempt.ClearedFindingFingerprints = slices.Compact(attempt.ClearedFindingFingerprints)
					slices.Sort(stage.ClearedFindingFingerprints)
					stage.ClearedFindingFingerprints = slices.Compact(stage.ClearedFindingFingerprints)
					stage.RemainingFindingFingerprints = slices.DeleteFunc(stage.RemainingFindingFingerprints, func(fp string) bool { return slices.Contains(attempt.ClearedFindingFingerprints, fp) })
					attempt.RemainingFindingFingerprints = nil
					delete(l.pendingWrites, id)
				}
				if _, live := l.applications[id]; live {
					l.applications[id] = app.Result
				}
				receipt, err := recordedPlanContract(*app)
				if err == nil {
					attempt.ApplyReceipt = receipt
				}
			}
		}
	}

	// One batch receipt cannot be successful for only its last binding. A
	// shared application earns positive credit only after every bound scope
	// independently settles; incomplete/partial batches retain their facts.
	for i := range applications {
		app := &applications[i]
		complete, found := true, false
		for _, stage := range stages {
			for _, attempt := range stage.Mitigations {
				if attempt.GetApplyReceipt().GetApplicationId() == app.ID {
					found = true
					complete = complete && attempt.Result == "applied_cleared"
				}
			}
		}
		if found && !complete {
			app.Result.VerificationPassed = false
			app.Result.FinalVerificationBasis = ""
		}
		if _, live := l.applications[app.ID]; live {
			l.applications[app.ID] = app.Result
		}
		receipt, err := recordedPlanContract(*app)
		if err != nil {
			continue
		}
		for _, stage := range stages {
			for _, attempt := range stage.Mitigations {
				if attempt.GetApplyReceipt().GetApplicationId() != app.ID {
					continue
				}
				if !complete && attempt.Result == "applied_cleared" {
					attempt.Result = "applied_still_matching"
				}
				attempt.ApplyReceipt = receipt
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
	positions := map[string]int{}
	for i, app := range result.PlanApplications {
		positions[app.ID] = i
	}
	for _, app := range settled.applications {
		if i, exists := positions[app.ID]; exists {
			result.PlanApplications[i] = app
		} else {
			positions[app.ID] = len(result.PlanApplications)
			result.PlanApplications = append(result.PlanApplications, app)
		}
	}
	for _, receipt := range ProgramRepairReceipts(settled.applications, nil) {
		generatedIDs[receipt.ActionID] = true
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
func recoveryObservationOnly(app RecordedPlanApplication) bool {
	if len(app.Result.ActionReports) != len(app.Result.Plan.Actions) {
		return false
	}
	for _, report := range app.Result.ActionReports {
		sourcePartial := report.SourceReconcile != nil && !report.SourceReconcile.Converged
		if report.RecordingError != "" || (report.FailureReason != "" && !(sourcePartial && report.FailureReason == "source_residual")) {
			return false
		}
		if report.Status != "applied" && !(sourcePartial && report.Status == "partial") {
			return false
		}
	}
	return true
}

// Reject overlapping later corrective effects, not independent causes or
// diagnostic helper writes. Unknown ordering is deliberately not positive proof.
func (l *ResponderLoop) recoverySuperseded(app RecordedPlanApplication, result IncidentResult, applications []RecordedPlanApplication, observedWrites []RepairActionReceipt, automatic bool) bool {
	if app.StartedAt.IsZero() || app.CompletedAt.IsZero() || len(result.RepairChanges) > 0 {
		return true
	}
	// Worker-returned applications are immutable history too; their absence
	// from this loop cannot prove that no competing corrective write occurred.
	seen := map[string]bool{}
	history := append([]RecordedPlanApplication(nil), applications...)
	for _, candidate := range applications {
		seen[candidate.ID] = true
	}
	for _, candidate := range result.PlanApplications {
		if !seen[candidate.ID] {
			history = append(history, candidate)
			seen[candidate.ID] = true
		}
	}
	generated := map[string]bool{}
	for _, receipt := range ProgramRepairReceipts(history, nil) {
		generated[receipt.ActionID] = true
	}
	own := ProgramRepairReceipts([]RecordedPlanApplication{app}, nil)
	overlaps := func(other RepairActionReceipt) bool {
		refs := append([]sdk.ObjectRef(nil), other.Resources...)
		for _, ownership := range other.Owners {
			refs = append(refs, ownership.OwnerChain...)
		}
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
	for _, other := range history {
		if writes := l.applicationWriteRefs(other.Result); writes != nil && len(*writes) == 0 {
			continue
		}
		if other.ID == app.ID || (!other.Result.Applied && len(other.Result.ActionsApplied) == 0) {
			continue
		}
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{other}, nil) {
			if overlaps(receipt) && (other.StartedAt.IsZero() || !other.StartedAt.Before(app.StartedAt)) {
				return true
			}
		}
	}
	for _, receipt := range observedWrites {
		if !overlaps(receipt) || (!receipt.CompletedAt.IsZero() && receipt.CompletedAt.Before(app.StartedAt)) {
			continue
		}
		// Automatic application writes precede the responder. API pairs fully
		// inside that interval duplicate its own execution, not a later actor.
		if automatic && !receipt.StartedAt.Before(app.StartedAt) && !receipt.CompletedAt.After(app.CompletedAt) {
			continue
		}
		return true
	}
	for _, receipt := range result.RepairActions {
		if generated[receipt.ActionID] {
			continue
		}
		if overlaps(receipt) && (receipt.StartedAt.IsZero() || !receipt.StartedAt.Before(app.StartedAt)) {
			return true
		}
	}
	return false
}

// Every downstream consumer checks the same durable exact settlement receipt.
func recoveryApplicationSucceeded(outcome *contracts.OutcomeRecord, app *contracts.PlanApplication, id, revision string) bool {
	return (app.GetFinalVerificationBasis() == "recovery-settled" || app.GetFinalVerificationBasis() == "observed-transition") && app.GetApplied() && !app.GetDryRun() && !app.GetRolledBack() && app.GetRollbackError() == "" && app.GetError() == "" && app.GetRolloutPending() == nil && app.GetVerificationCompleted() && app.GetVerificationPassed() && finalApplicationVerified(app) && boundPostApplyNoMatch(outcome, app, id, revision)
}

// Deleted objects have no final fields. Their exact effect is a recorded
// UID-bound delete followed by a fresh direct NotFound, never a failed GET.
// Remaining document effects use the existing field-equivalence predicate.
func (l *ResponderLoop) recoveryPlanFieldsMatch(result PlanApplicationResult, getFinal func(sdk.ObjectRef) *unstructured.Unstructured, absent func(sdk.ObjectRef) bool) bool {
	if writes := l.applicationWriteRefs(result); writes != nil {
		original := result.PreImages
		result.PreImages = nil
		for _, image := range original {
			if slices.Contains(*writes, image.Ref) {
				result.PreImages = append(result.PreImages, image)
			}
		}
	}
	if len(result.PreImages) == 0 {
		return false
	}
	remaining := result
	remaining.PreImages = nil
	for _, image := range result.PreImages {
		deleted := false
		for _, effect := range result.ObjectEffects {
			if effect.Ref == image.Ref && effect.Operation == "delete" && effect.State == "applied" && effect.Confirmed && image.Object != nil && effect.UID != "" && effect.UID == string(image.Object.GetUID()) {
				if !absent(image.Ref) {
					return false
				}
				deleted = true
			}
		}
		if !deleted {
			remaining.PreImages = append(remaining.PreImages, image)
		}
	}
	return len(remaining.PreImages) == 0 || finalPlanFieldsMatch(remaining, getFinal)
}

// Recovery bindings are immutable observation facts, never prepared plan IDs.
// The controller records them before a write; restart restores no executor handle.
type recoveryBinding struct {
	ActionStart         int
	ActionCount         int
	ProgramID           string
	ProgramRevision     string
	Bindings            program.Bindings
	FindingFingerprints []string
}

// Agent-owned applications use the same report/settlement contract as automatic
// applications. A missing or ambiguous binding stays unknown, never inferred.
func (l *ResponderLoop) recoveryStageEvidence(stages []*contracts.StageReport) []*contracts.StageReport {
	stages = cloneStageReports(stages)
	if l.Programs == nil {
		return stages
	}
	seen := map[string]bool{}
	for _, stage := range stages {
		for _, attempt := range stage.Mitigations {
			seen[attempt.GetApplyReceipt().GetApplicationId()] = true
		}
	}
	var agent *contracts.StageReport
	for _, stage := range stages {
		if stage.Stage == "agent" {
			agent = stage
			break
		}
	}
	for _, app := range l.applicationEvidenceLocked() {
		bindings := l.applicationBindings[app.ID]
		if seen[app.ID] || len(bindings) == 0 || len(app.ProgramIDs) != len(bindings) {
			continue
		}
		valid := true
		for i, binding := range bindings {
			_, registered := l.Programs.Diagnoser(binding.ProgramID)
			valid = valid && registered && binding.ProgramRevision != "" && binding.ProgramID == app.ProgramIDs[i] && binding.ActionStart >= 0 && binding.ActionCount >= 0 && binding.ActionStart+binding.ActionCount <= len(app.Result.Plan.Actions)
		}
		if !valid {
			continue
		}
		receipt, err := recordedPlanContract(app)
		if err != nil {
			continue
		}
		if agent == nil {
			agent = &contracts.StageReport{Stage: "agent"}
			stages = append(stages, agent)
		}
		for _, binding := range bindings {
			count := binding.ActionCount
			if count == 0 && len(bindings) == 1 {
				count = len(app.Result.Plan.Actions)
			}
			agent.Mitigations = append(agent.Mitigations, &contracts.StageMitigation{PlaybookId: binding.ProgramID, ProgramRevision: binding.ProgramRevision, Bindings: programBindingsContract(binding.Bindings), ApplyReceipt: receipt, PlanActionStart: int32(binding.ActionStart), PlanActionCount: int32(count), RemainingFindingFingerprints: append([]string(nil), binding.FindingFingerprints...), Result: "rollout_pending_at_handoff"})
		}
		seen[app.ID] = true
	}
	return stages
}

// Raw worker API writes are recorded outside tool receipts. Observe the existing
// storage-edge projection before credit, rather than discovering interference
// only later during outcome derivation. Incomplete history stays unknown.
func (l *ResponderLoop) recoveryAPIWrites(ctx context.Context, result IncidentResult, applications []RecordedPlanApplication) ([]RepairActionReceipt, bool) {
	if l.RecoverySessionRoot == "" || result.ResponderSessionID == "" {
		return nil, true
	}
	var since time.Time
	var resources []*contracts.ObjectRef
	for _, app := range applications {
		if since.IsZero() || (!app.StartedAt.IsZero() && app.StartedAt.Before(since)) {
			since = app.StartedAt
		}
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{app}, nil) {
			for _, ref := range receipt.Resources {
				resources = append(resources, objectRefContract(ref))
			}
		}
	}
	if len(resources) == 0 {
		return nil, true
	}
	observed, err := memory.IncidentAPIRepairReceipts(l.RecoverySessionRoot, l.Request.IncidentID, resources, since, time.Now())
	if err != nil {
		slog.WarnContext(ctx, "application_recovery_history_unavailable", "incident_id", l.Request.IncidentID, "error", err)
		return nil, false
	}
	return incidentResultState(&contracts.IncidentResult{RepairActions: observed}).RepairActions, true
}
