package runtime

import (
	"encoding/json"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func atomicDocumentPlan(plan program.Plan) bool {
	seen := map[sdk.ObjectRef]bool{}
	repeated := false
	for _, a := range plan.Actions {
		ref, ok := program.DocumentWriteRef(a)
		if !ok {
			return false
		}
		repeated = repeated || seen[ref]
		seen[ref] = true
	}
	return repeated
}

// Object order is first occurrence; within an object, authored order wins.
// Mixed effect plans are never reordered (Plan.Validate refuses coupled edits).
func groupDocumentActions(plan program.Plan) program.Plan {
	if !atomicDocumentPlan(plan) {
		return plan
	}
	groups := map[sdk.ObjectRef][]program.Action{}
	var refs []sdk.ObjectRef
	for _, a := range plan.Actions {
		ref, _ := program.DocumentWriteRef(a)
		if _, ok := groups[ref]; !ok {
			refs = append(refs, ref)
		}
		groups[ref] = append(groups[ref], a)
	}
	plan.Actions = nil
	for _, ref := range refs {
		plan.Actions = append(plan.Actions, groups[ref]...)
	}
	return plan
}

// Keep one image slot per logical action for existing rollback/report indexing.
// Only the first slot owns the aggregate delta; followers own no field changes.
func coalesceDocumentPatches(result *PlanApplicationResult) error {
	for first := 0; first < len(result.Patches); {
		end := first + 1
		for end < len(result.Patches) && result.Patches[end].Ref == result.Patches[first].Ref {
			end++
		}
		patch := &result.Patches[first]
		patch.ActionCount = end - first
		patch.After = result.Patches[end-1].After.DeepCopy()
		if end-first > 1 {
			patch.Authorization = "general"
		}
		for i := first; i < end; i++ {
			if isGeneralAction(result.Plan.Actions[i]) {
				patch.Authorization = "general"
				patch.Structured = true
			}
			patch.Structured = patch.Structured || result.Patches[i].Structured
		}
		before, err := json.Marshal(patch.Before.Object)
		if err != nil {
			return err
		}
		after, err := json.Marshal(patch.After.Object)
		if err != nil {
			return err
		}
		patch.Patch, err = createConditionalObjectPatch(before, after, patch.Before)
		if err != nil {
			return err
		}
		for i := first + 1; i < end; i++ {
			result.Patches[i].Before = patch.After.DeepCopy()
			result.Patches[i].After = patch.After.DeepCopy()
			result.Patches[i].Patch = json.RawMessage(`{}`)
		}
		first = end
	}
	return nil
}
