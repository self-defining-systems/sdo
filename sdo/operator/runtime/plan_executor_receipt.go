package runtime

import (
	"context"
	"fmt"
)

// Reconcile resolves an uncertain API receipt within this process. Its input
// is never restored from durable storage.
func (e PlanExecutor) Reconcile(ctx context.Context, result PlanApplicationResult) (PlanApplicationResult, error) {
	if result.PendingAction == nil {
		return result, nil
	}
	index := *result.PendingAction
	if index < 0 || index >= len(result.Patches) || index >= len(result.Plan.Actions) {
		return result, fmt.Errorf("invalid pending application action %d", index)
	}
	patch := result.Patches[index]
	if patch.Operation != "" {
		return e.reconcileObjectEffect(ctx, result, index)
	}
	if patch.Before == nil || patch.After == nil {
		return result, fmt.Errorf("pending write has no in-process field images")
	}
	live, err := e.Client.Get(ctx, patch.Ref)
	if err != nil {
		return result, err
	}
	if patch.Structured {

		if live.GetUID() == "" || live.GetUID() != patch.Before.GetUID() {
			return result, fmt.Errorf("structured receipt unknown: UID changed")
		}
		before, after := true, true
		for _, field := range planPatchFields(patch) {
			b, bok := planConfigurationField(patch.Before, field)
			a, aok := planConfigurationField(patch.After, field)
			v, vok := live.Object[field]
			before = before && planFieldsMatch(b, bok, a, aok, v, vok, true)
			after = after && planFieldsMatch(b, bok, a, aok, v, vok, false)
		}
		if !before && !after {
			return result, fmt.Errorf("structured receipt unknown: neither field image matches")
		}
	}
	// The write landed unless the object still holds its pre-image. Anything else
	// is the write plus whatever the server made of it, so no after-image is compared.
	landed := false
	for _, field := range planPatchFields(patch) {
		b, bok := planConfigurationField(patch.Before, field)
		a, aok := planConfigurationField(patch.After, field)
		v, vok := live.Object[field]
		landed = landed || !planFieldsMatch(b, bok, a, aok, v, vok, true)
	}
	if landed && len(result.ActionsApplied) == index {
		count := patch.ActionCount
		if count < 1 {
			count = 1
		}
		if index+count > len(result.Plan.Actions) {
			return result, fmt.Errorf("invalid pending action count")
		}
		result.ActionsApplied = append(result.ActionsApplied, result.Plan.Actions[index:index+count]...)
		result.Applied = true
	}
	result.PendingAction = nil
	return result, nil
}

func planFieldsMatch(before any, bok bool, after any, aok bool, live any, lok bool, wantBefore bool) bool {
	if bok == aok && planValuesEqual(before, after) {
		return true
	}
	bm, bmap := before.(map[string]any)
	am, amap := after.(map[string]any)
	lm, lmap := live.(map[string]any)
	if bmap && amap && lmap {
		keys := map[string]bool{}
		for k := range bm {
			keys[k] = true
		}
		for k := range am {
			keys[k] = true
		}
		for k := range keys {
			b, bo := bm[k]
			a, ao := am[k]
			l, lo := lm[k]
			if !planFieldsMatch(b, bo, a, ao, l, lo, wantBefore) {
				return false
			}
		}
		return true
	}
	ba, barray := before.([]any)
	aa, aarray := after.([]any)
	la, larray := live.([]any)
	if barray && aarray && larray && len(ba) == len(aa) && len(aa) == len(la) {
		for i := range ba {
			if !planFieldsMatch(ba[i], true, aa[i], true, la[i], true, wantBefore) {
				return false
			}
		}
		return true
	}
	if wantBefore {
		return bok == lok && planValuesEqual(before, live)
	}
	if bok && !aok && lok {
		// The write removed a field the server defaults back.
		return planValuesEqual(before, live)
	}
	return aok == lok && planValuesEqual(after, live)
}
