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
		refs := []sdk.ObjectRef{}
		seen := map[sdk.ObjectRef]bool{}
		actions := application.Result.Plan.Actions
		if len(actions) == 0 {
			actions = application.Result.ActionsApplied
		}
		for index, action := range actions {
			ref, err := planActionRef(action)
			if err != nil || ref.Kind == "" {
				continue
			}
			applied := index < len(application.Result.ActionsApplied)
			reason := ""
			if index < len(application.Result.ActionReports) {
				report := application.Result.ActionReports[index]
				applied = report.Status == "applied"
				reason = report.FailureReason
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
				result = append(result, RepairActionReceipt{ActionID: id, Kind: "program-plan", Target: ref.Kind + "/" + ref.Name, Resources: []sdk.ObjectRef{ref}, Summary: "program plan action failed", Details: fmt.Sprintf("controller action %d in %s: %s", index+1, application.ID, reason), StartedAt: application.StartedAt, CompletedAt: application.CompletedAt, FailureReason: reason})
				continue
			}
			if applied && application.Result.Applied && !seen[ref] {
				refs = append(refs, ref)
				seen[ref] = true
			}
		}
		if len(refs) == 0 {
			continue
		}
		id := uniqueRepairID("sdo-program-"+application.ID, used)
		result = append(result, RepairActionReceipt{
			ActionID: id, Kind: "program-plan", Target: refs[0].Kind + "/" + refs[0].Name, Resources: refs,
			Summary: "applied program plan " + strings.Join(application.ProgramIDs, ", "), Details: fmt.Sprintf("controller confirmed %d applied resource effects in %s", len(refs), application.ID),
			StartedAt: application.StartedAt, CompletedAt: application.CompletedAt,
			Success: !application.RolledBack && application.RollbackError == "", Reversible: len(application.Result.PreImages) >= len(refs),
		})
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
