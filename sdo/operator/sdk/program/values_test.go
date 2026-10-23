package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestValueRequiresProvenance(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"}
	for _, v := range []program.Value{program.FromSource(ref, "data[mode]"), program.FromObserved(ref, "data[mode]"), program.FromParam("mode")} {
		if err := v.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	if err := (program.Value{}).Validate(); err == nil {
		t.Fatal("zero value accepted")
	}
	if err := program.FromSource(sdk.ObjectRef{Kind: "Secret"}, "data[key]").Validate(); err == nil {
		t.Fatal("Secret reference accepted")
	}
}

func TestValueParameterKinds(t *testing.T) {
	for _, tc := range []struct {
		kind      program.ValueKind
		good, bad any
	}{
		{program.StringValue, "safe", true}, {program.IntValue, 2, 2.5}, {program.QuantityValue, "128Mi", "bad"}, {program.DurationValue, "2s", "bad"}, {program.BoolValue, false, "false"},
	} {
		if err := program.ValidateValueBinding(program.ParamSpec{Name: "value", ValueKind: tc.kind}, tc.good); err != nil {
			t.Fatal(err)
		}
		if err := program.ValidateValueBinding(program.ParamSpec{Name: "value", ValueKind: tc.kind}, tc.bad); err == nil {
			t.Fatalf("accepted wrong %s value", tc.kind)
		}
	}
}
