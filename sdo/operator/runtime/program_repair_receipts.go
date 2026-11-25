package runtime

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// ProgramRepairReceipts records controller-applied mutations independently of
// the responder's own account of live actions.
func ProgramRepairReceipts(applications []RecordedPlanApplication, existing []RepairActionReceipt) []RepairActionReceipt {
	result := append([]RepairActionReceipt(nil), existing...)
	used := map[string]bool{}
	for _, receipt := range existing {
		used[receipt.ActionID] = true
	}
	for _, application := range applications {
		started, completed := receiptTimes(application)
		// failure names why the application's receipts are not successful.
		// Empty means it took effect and nothing failed: its own verification
		// passed, or it never ran one because the responder verifies the
		// incident as a whole. A receipt reads FAILED only with a reason.
		failure := ""
		residualOnly := sourceResidualOnly(application)
		switch {
		case application.RollbackError != "":
			failure = "rollback_failed"
		case application.RolledBack:
			failure = "rolled_back"
		case application.Error != "" && !residualOnly:
			failure = "application_error"
		case application.Result.SettlementRejectionReason != contracts.SettlementRejectionReason_SETTLEMENT_REJECTION_REASON_UNSPECIFIED:
			failure = "settlement_rejected"
		case application.Pending || planApplicationPending(application.Result):
			failure = "application_pending"
		case application.Result.VerificationCompleted && !application.Result.VerificationPassed:
			failure = "verification_failed"
		}
		verified := failure == "" && application.Result.VerificationCompleted && application.Result.VerificationPassed
		// A source reconcile that wrote the object but left residual
		// differences is recorded evidence in its action report, not a failed
		// repair: whether it fixed anything is the alert set's verdict.
		verified = verified || (residualOnly && failure == "")
		refs := []sdk.ObjectRef{}
		seen := map[sdk.ObjectRef]bool{}
		actions := application.Result.Plan.Actions
		if len(actions) == 0 {
			actions = application.Result.ActionsApplied
		}
		for index, action := range actions {
			if script, isScript := concreteExternalAction(action).(program.RunScript); isScript && index < len(application.Result.ActionReports) {
				report := application.Result.ActionReports[index]
				scoped := scriptRepairResources(report)
				if report.Status == "applied" && application.Result.Applied && report.Script != nil && report.Script.ScriptDigest == program.ScriptDigest(script.Script) && !application.RolledBack && application.RollbackError == "" {
					id := uniqueRepairID(fmt.Sprintf("sdo-program-%s-script-%d", application.ID, index+1), used)
					result = append(result, RepairActionReceipt{ActionID: id, Kind: "run-script", ScriptDigest: report.Script.ScriptDigest, Resources: scoped, Target: "script", Summary: "completed script", Details: "controller-recorded exact script digest", StartedAt: started, CompletedAt: completed, Success: verified, FailureReason: failure})
					for _, ref := range scoped {
						if !seen[ref] {
							seen[ref] = true
							refs = append(refs, ref)
						}
					}
				}
				if report.FailureReason != "" {
					id := uniqueRepairID(fmt.Sprintf("sdo-program-%s-failed-%d", application.ID, index+1), used)
					result = append(result, RepairActionReceipt{ActionID: id, Kind: "run-script", Target: "script", Resources: scoped, Summary: "script execution failed or outcome unknown", Details: failedScriptDetails(index, application.ID, report), StartedAt: started, CompletedAt: completed, FailureReason: report.FailureReason})
				}
				continue
			}
			ref, err := planActionRef(action)
			if err != nil || ref.Kind == "" {
				continue
			}
			// Action reports prove completion, including successful observations.
			// Only actual mutations (or confirmed effects below) own resources.
			applied := slices.ContainsFunc(application.Result.ActionsApplied, func(mutation program.Action) bool {
				return reflect.DeepEqual(action, mutation)
			})
			reason := ""
			if index < len(application.Result.ActionReports) {
				report := application.Result.ActionReports[index]
				applied = applied && (report.Status == "applied" || (residualOnly && report.Status == "partial"))
				reason = report.FailureReason
				if residualOnly && reason == "source_residual" {
					reason = ""
				}
				if report.Status == "unknown" && reason == "" {
					reason = "outcome_unknown"
				}
			} else if application.Error != "" && applied {
				reason = "outcome_unknown"
			}
			for _, effect := range application.Result.ObjectEffects {
				if effect.Ref == ref && !effect.Confirmed {
					applied = false
					reason = "outcome_unknown"
				}
			}
			if reason != "" {
				id := uniqueRepairID(fmt.Sprintf("sdo-program-%s-failed-%d", application.ID, index+1), used)
				result = append(result, RepairActionReceipt{ActionID: id, Kind: "program-plan", Target: ref.Kind + "/" + ref.Name, Resources: []sdk.ObjectRef{ref}, Summary: "program plan action failed", Details: fmt.Sprintf("controller action %d in %s: %s", index+1, application.ID, reason), StartedAt: started, CompletedAt: completed, FailureReason: reason})
				continue
			}
			if applied && application.Result.Applied && !seen[ref] {
				refs = append(refs, ref)
				seen[ref] = true
			}
		}
		irreversible := false
		for _, action := range actions {
			irreversible = irreversible || isAdmissionRetry(action)
		}
		for _, effect := range application.Result.ObjectEffects {
			irreversible = irreversible || effect.Irreversible
			if effect.Confirmed && effect.State == "applied" && !seen[effect.Ref] {
				refs = append(refs, effect.Ref)
				seen[effect.Ref] = true
			}
		}
		if len(refs) == 0 {
			continue
		}
		id := uniqueRepairID("sdo-program-"+application.ID, used)
		summary := "applied program plan " + strings.Join(application.ProgramIDs, ", ")
		details := fmt.Sprintf("controller confirmed %d applied resource effects in %s", len(refs), application.ID)
		if hasAppliedScript(actions) {
			summary = "completed opaque script plan " + strings.Join(application.ProgramIDs, ", ")
			details = fmt.Sprintf("controller recorded script completion for %d selected/touched resources in %s; external effects remain opaque", len(refs), application.ID)
		}
		result = append(result, RepairActionReceipt{
			ActionID: id, Kind: "program-plan", Target: refs[0].Kind + "/" + refs[0].Name, Resources: refs,
			Summary: summary, Details: details,
			StartedAt: started, CompletedAt: completed,
			Success: verified, FailureReason: failure, Reversible: !irreversible && !hasAppliedScript(actions) && len(application.Result.PreImages) >= len(refs),
			Owners: preImageOwners(application.Result.PreImages, refs),
		})
	}
	return result
}

// receiptTimes is the application's interval as a receipt records it. A pending
// application has not completed, so its receipt ends where it started.
func receiptTimes(application RecordedPlanApplication) (time.Time, time.Time) {
	started, completed := application.StartedAt, application.CompletedAt
	if started.IsZero() {
		started = completed
	}
	if completed.Before(started) {
		completed = started
	}
	return started, completed
}

func failedScriptDetails(index int, applicationID string, report PlanActionReport) string {
	details := fmt.Sprintf("controller action %d in %s: %s", index+1, applicationID, report.FailureReason)
	if report.ExitCode != nil {
		details += fmt.Sprintf(" (exit %d)", *report.ExitCode)
	}
	return details
}

// sourceResidualOnly reports whether an application's only error is a source
// reconcile that applied but kept residual differences: every failing action
// report names source_residual, nothing rolled back, and the executor's error
// is the partial-reconcile one.
func sourceResidualOnly(application RecordedPlanApplication) bool {
	if !application.Result.Applied || application.RolledBack || application.RollbackError != "" {
		return false
	}
	if application.Error != "" && !strings.HasPrefix(application.Error, "source_reconcile_partial:") {
		return false
	}
	residual := false
	for _, report := range application.Result.ActionReports {
		switch report.FailureReason {
		case "":
		case "source_residual":
			residual = true
		default:
			return false
		}
	}
	return residual
}

// preImageOwners records each written resource's controller owner chain from
// the objects read before the write. A chain extends only through owners
// whose own pre-image is present; it is never completed from names.
func preImageOwners(preImages []PlanPreImage, refs []sdk.ObjectRef) []ResourceOwners {
	controller := func(ref sdk.ObjectRef) (sdk.ObjectRef, bool) {
		for _, image := range preImages {
			if image.Object == nil || image.Ref.Kind != ref.Kind || image.Ref.Namespace != ref.Namespace || image.Ref.Name != ref.Name {
				continue
			}
			for _, owner := range image.Object.GetOwnerReferences() {
				if owner.Controller != nil && *owner.Controller && owner.Kind != "" && owner.Name != "" {
					return sdk.ObjectRef{APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: ref.Namespace, Name: owner.Name}, true
				}
			}
		}
		return sdk.ObjectRef{}, false
	}
	var result []ResourceOwners
	for _, ref := range refs {
		var chain []sdk.ObjectRef
		for next, ok := controller(ref); ok && len(chain) < 4; next, ok = controller(next) {
			chain = append(chain, next)
		}
		if len(chain) > 0 {
			result = append(result, ResourceOwners{Resource: ref, OwnerChain: chain})
		}
	}
	return result
}

func uniqueRepairID(base string, used map[string]bool) string {
	id := base
	for suffix := 2; used[id]; suffix++ {
		id = fmt.Sprintf("%s-%d", base, suffix)
	}
	used[id] = true
	return id
}

// ApplicationRepairReceipts projects immutable application facts, including
// receipts restored before a worker returned. Restoring a receipt grants no
// plan handle or rollback authority. Controller receipts take precedence over
// an agent echoing the same action ID.
func (l *ResponderLoop) ApplicationRepairReceipts(existing []RepairActionReceipt) []RepairActionReceipt {
	var completed []continuationRepair
	l.locked(func() { completed = l.applicationRepairsLocked() })
	result := make([]RepairActionReceipt, 0, len(completed)+len(existing))
	seen := map[string]bool{}
	for _, repair := range completed {
		receipt := repair.Receipt
		result = append(result, repairActionFromContinuation(receipt))
		seen[receipt.ActionID] = true
	}
	for _, receipt := range existing {
		if !seen[receipt.ActionID] {
			result = append(result, receipt)
			seen[receipt.ActionID] = true
		}
	}
	return result
}

// repairActionFromContinuation is shared by normal and early closure so the
// same persisted controller facts reach either outcome path.
func repairActionFromContinuation(receipt continuationReceipt) RepairActionReceipt {
	return RepairActionReceipt{ActionID: receipt.ActionID, Kind: receipt.Kind, Target: receipt.Target, Resources: append([]sdk.ObjectRef(nil), receipt.Resources...), Summary: receipt.Summary, Details: receipt.Details, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt, Success: receipt.Success, Reversible: receipt.Reversible, ScriptDigest: receipt.ScriptDigest, FailureReason: receipt.FailureReason, Owners: append([]ResourceOwners(nil), receipt.Owners...), RunningCommit: receipt.RunningCommit, RunningDirty: receipt.RunningDirty}
}
