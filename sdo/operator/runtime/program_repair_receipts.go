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
		if !application.Result.Applied {
			continue
		}
		refs := make([]sdk.ObjectRef, 0)
		seen := map[sdk.ObjectRef]bool{}
		for _, action := range application.Result.ActionsApplied {
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
			Success: true, Reversible: len(application.Result.PreImages) >= len(refs),
		})
	}
	return result
}
