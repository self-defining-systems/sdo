package runtime

import (
	"fmt"
	"strings"

	"sdo.dev/operator/sdk"
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
		failureReason := ""
		confirmed := application.Result.PendingAction == nil
		for _, report := range application.Result.ActionReports {
			if report.Status != "applied" {
				confirmed = false
			}
			if report.FailureReason != "" {
				failureReason = report.FailureReason
			}
		}
		for _, effect := range application.Result.ObjectEffects {
			if !effect.Confirmed {
				confirmed = false
			}
		}
		if !confirmed && failureReason == "" {
			failureReason = "outcome_unknown"
		}
		if !application.Result.Applied && failureReason == "" {
			continue
		}
		actions := application.Result.ActionsApplied
		if !application.Result.Applied && failureReason != "" {
			actions = application.Result.Plan.Actions
		}
		refs := make([]sdk.ObjectRef, 0)
		seen := map[sdk.ObjectRef]bool{}
		for _, action := range actions {
			ref, err := planActionRef(action)
			if err == nil && !seen[ref] {
				refs = append(refs, ref)
				seen[ref] = true
			}
		}
		if len(refs) == 0 {
			continue
		}
		id := "sdo-program-" + application.ID
		for suffix := 2; used[id]; suffix++ {
			id = fmt.Sprintf("sdo-program-%s-%d", application.ID, suffix)
		}
		used[id] = true
		result = append(result, RepairActionReceipt{
			ActionID: id, Kind: "program-plan", Target: refs[0].Kind + "/" + refs[0].Name,
			Resources: refs, Summary: "applied program plan " + strings.Join(application.ProgramIDs, ", "),
			Details:   fmt.Sprintf("controller applied %d plan actions in %s", len(application.Result.ActionsApplied), application.ID),
			StartedAt: application.StartedAt, CompletedAt: application.CompletedAt,
			FailureReason: failureReason, Success: application.Result.Applied && confirmed && !application.RolledBack && application.RollbackError == "", Reversible: len(application.Result.PreImages) >= len(refs),
		})
	}
	return result
}

// ApplicationRepairReceipts projects immutable application facts, including
// receipts restored before a worker returned. Restoring a receipt grants no
// plan handle or rollback authority. Controller receipts take precedence over
// an agent echoing the same action ID.
func (l *ResponderLoop) ApplicationRepairReceipts(existing []RepairActionReceipt) []RepairActionReceipt {
	l.mu.Lock()
	completed := l.applicationRepairsLocked()
	l.mu.Unlock()
	result := make([]RepairActionReceipt, 0, len(completed)+len(existing))
	seen := map[string]bool{}
	for _, repair := range completed {
		receipt := repair.Receipt
		result = append(result, RepairActionReceipt{ActionID: receipt.ActionID, Kind: receipt.Kind, Target: receipt.Target, Resources: append([]sdk.ObjectRef(nil), receipt.Resources...), Summary: receipt.Summary, Details: receipt.Details, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt, Success: receipt.Success, Reversible: receipt.Reversible, FailureReason: receipt.FailureReason})
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
