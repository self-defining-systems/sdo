package replay

import (
	"bytes"
	"context"
	"fmt"

	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// checkTargetValues supplements executor value read-back with the corpus's
// intended target. Source and parameter writes must restore that accepted path;
// observed writes retain the executor's frozen-observation provenance checks.
// Shared SDK resolvers/document editors are reused, not action implementations.
func checkTargetValues(ctx context.Context, plan program.Plan, application operator.PlanApplicationResult, source *source) error {
	for index, action := range plan.Actions {
		field, ok := valueField(action)
		if !ok || field.Value.Origin() == program.ObservedValue {
			continue
		}
		if index >= len(application.Patches) || application.Patches[index].After == nil {
			return fmt.Errorf("requested target lacks executor after-image")
		}
		patch := application.Patches[index]
		expected := patch.After.DeepCopy()
		requested := field
		requested.Value = program.FromSource(field.Ref, field.Path)
		value, _, err := program.ResolveSetFieldValue(requested, program.ValueInputs{SourceCommit: "offline-accepted", Source: func(ref sdk.ObjectRef) (map[string]any, error) {
			o, e := source.AcceptedManifest(ctx, "offline-accepted", ref)
			if e != nil {
				return nil, e
			}
			return o.Object, nil
		}})
		if err != nil {
			return fmt.Errorf("accepted target value unavailable: %s/%s %s", field.Ref.Kind, field.Ref.Name, field.Path)
		}
		if err = program.ApplySetFieldAction(expected.Object, field, value); err != nil {
			return fmt.Errorf("accepted target cannot be compared: %s/%s %s", field.Ref.Kind, field.Ref.Name, field.Path)
		}
		left, le := comparable(expected)
		right, re := comparable(patch.After)
		if le != nil || re != nil || !bytes.Equal(left, right) {
			return fmt.Errorf("requested field differs from accepted target: %s/%s %s", field.Ref.Kind, field.Ref.Name, field.Path)
		}
	}
	return nil
}
func valueField(action program.Action) (program.SetField, bool) {
	switch a := action.(type) {
	case program.SetField:
		return a, true
	case program.ScaleWorkload:
		return program.SetField{Ref: a.Ref, Path: "spec.replicas", Value: a.Replicas}, true
	case *program.SetField:
		if a != nil {
			return *a, true
		}
	case *program.ScaleWorkload:
		if a != nil {
			return valueField(*a)
		}
	}
	return program.SetField{}, false
}
