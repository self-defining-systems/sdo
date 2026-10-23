package runtime

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// testPlanValue supplies a provenance-bound, nonsecret observation fixture.
// Production programs use their real ReadView snapshot or declared parameters.
func testPlanValue(value any) program.Value {
	kind := program.IntValue
	scalar := true
	switch value.(type) {
	case string:
		kind = program.StringValue
	case bool:
		kind = program.BoolValue
	case int, int32, int64, float64:
	default:
		scalar = false
	}
	if scalar {
		plan := program.Plan{Actions: []program.Action{program.SetField{Value: program.FromParam("fixture")}}}
		bound := program.BindPlanValues(plan, program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "fixture", ValueKind: kind, Default: value}}}})
		return bound.Actions[0].(program.SetField).Value
	}

	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "fixture", Name: "value"}
	p := program.BindPlanValues(program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[value]", Value: program.FromObserved(ref, "data[value]")}}}, program.ValueInputs{Observed: func(sdk.ObjectRef) (map[string]any, error) {
		return map[string]any{"metadata": map[string]any{"resourceVersion": "fixture"}, "data": map[string]any{"value": value}}, nil
	}})
	return p.Actions[0].(program.SetField).Value
}
