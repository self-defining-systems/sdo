package runtime

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func structuredPlanAction(action program.Action) bool {
	switch a := action.(type) {
	case program.CreateFromSource, *program.CreateFromSource, program.DeleteObject, *program.DeleteObject:
		return true
	case program.SetField:
		return program.ExpandedSetField(a)
	case *program.SetField:
		return a != nil && program.ExpandedSetField(*a)
	}
	ref, err := planActionRef(action)
	return err == nil && (ref.Kind == "NetworkPolicy" || ref.Kind == "HorizontalPodAutoscaler" || ref.Kind == "PodDisruptionBudget")
}
func structuredPlanRef(plan program.Plan, ref sdk.ObjectRef) bool {
	for _, a := range plan.Actions {
		target, err := planActionRef(a)
		if err == nil && target == ref && structuredPlanAction(a) {
			return true
		}
	}
	return false
}

// verifyStructuredWrites proves intended fields with a direct current read.
// It never substitutes an informer timeout or a stale verifier for write proof.
func (e PlanExecutor) verifyStructuredWrites(ctx context.Context, result PlanApplicationResult) error {
	for _, patch := range result.Patches {
		if !patch.Structured {
			continue
		}
		if patch.Operation != "" {
			if err := e.observeObjectEffect(ctx, patch); err != nil {
				return err
			}
			continue
		}
		if err := e.Safety.authorizeRead(patch.Ref, patch.SourceCommit, e.safetyNow()); err != nil {
			return err
		}
		live, err := e.Client.Get(ctx, patch.Ref)
		if err != nil {
			return fmt.Errorf("structured write observation unknown: current read failed")
		}
		if err := e.Safety.checkObservedObject(live, patch.After, patch.Ref, false); err != nil {
			return err
		}
		if live.GetUID() == "" || live.GetUID() != patch.Before.GetUID() {
			return fmt.Errorf("structured write observation unknown: UID changed or absent")
		}
		for _, field := range []string{"spec", "data", "binaryData"} {
			b, bok := patch.Before.Object[field]
			a, aok := patch.After.Object[field]
			v, vok := live.Object[field]
			if !planFieldsMatch(b, bok, a, aok, v, vok, false) {
				return fmt.Errorf("structured write observation unknown: intended field value not observed")
			}
		}
	}
	return nil
}
