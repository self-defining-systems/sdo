package runtime

import (
	"context"
	"fmt"
)

// Reconcile determines whether a write interrupted before its receipt reached
// durable storage took effect. It never replays a forward mutation.
func (e PlanExecutor) Reconcile(ctx context.Context, result PlanApplicationResult) (PlanApplicationResult, error) {
	if result.PendingAction == nil {
		return result, nil
	}
	index := *result.PendingAction
	if index < 0 || index >= len(result.Patches) || index >= len(result.Plan.Actions) {
		return result, fmt.Errorf("invalid pending application action %d", index)
	}
	patch := result.Patches[index]
	if patch.Before == nil || patch.After == nil {
		return result, fmt.Errorf("pending write has no field journal")
	}
	live, err := e.Client.Get(ctx, patch.Ref)
	if err != nil {
		return result, err
	}
	if live.GetUID() != patch.Before.GetUID() {
		return result, fmt.Errorf("interrupted write conflict: object recreated")
	}
	matchesAfter := true
	matchesBefore := true
	for _, field := range []string{"spec", "data", "binaryData"} {
		b, bok := patch.Before.Object[field]
		a, aok := patch.After.Object[field]
		v, vok := live.Object[field]
		matchesAfter = matchesAfter && planFieldsMatch(b, bok, a, aok, v, vok, false)
		matchesBefore = matchesBefore && planFieldsMatch(b, bok, a, aok, v, vok, true)
	}
	if !matchesAfter && !matchesBefore {
		return result, fmt.Errorf("interrupted write conflict: owned fields changed")
	}
	if matchesAfter && !matchesBefore && len(result.ActionsApplied) == index {
		result.ActionsApplied = append(result.ActionsApplied, result.Plan.Actions[index])
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
	return aok == lok && planValuesEqual(after, live)
}
