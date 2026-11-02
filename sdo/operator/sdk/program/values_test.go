package program_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"testing/quick"
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

// An untyped Go constant is assignable only to basic or interface types, so a
// struct-typed SetField.Value rejects raw literals at compile time. Checking the
// field type replaces a `go build` subprocess that raced a fixed timeout on a
// cold build cache.
func TestSetFieldLiteralDoesNotCompile(t *testing.T) {
	field, ok := reflect.TypeOf(program.SetField{}).FieldByName("Value")
	if !ok {
		t.Fatal("SetField has no Value field")
	}
	if field.Type != reflect.TypeOf(program.Value{}) || field.Type.Kind() != reflect.Struct {
		t.Fatalf("SetField.Value is %s (%s); a raw literal would compile", field.Type, field.Type.Kind())
	}
}

func TestValueRenameInvarianceAndPerturbation(t *testing.T) {
	property := func(n uint8) bool {
		for _, origin := range []program.ValueOrigin{program.SourceValue, program.ObservedValue} {
			var previous any
			for _, name := range []string{"settings", "renamed-settings"} {
				ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: name}
				value := program.FromSource(ref, "data[replicas]")
				if origin == program.ObservedValue {
					value = program.FromObserved(ref, "data[replicas]")
				}
				var first any
				for index, replicas := range []int{int(n) + 1, int(n) + 2} {
					read := func(got sdk.ObjectRef) (map[string]any, error) {
						if got != ref {
							return nil, fmt.Errorf("wrong renamed reference")
						}
						return map[string]any{"metadata": map[string]any{"resourceVersion": "17"}, "data": map[string]any{"replicas": replicas}}, nil
					}
					resolved, p, err := program.ResolveValue(value, program.ValueInputs{SourceCommit: "accepted", Source: read, Observed: read})
					if err != nil || resolved != replicas || p.Ref != ref || p.Path != "data[replicas]" {
						return false
					}
					if origin == program.SourceValue && p.SourceCommit != "accepted" || origin == program.ObservedValue && p.ResourceVersion != "17" {
						return false
					}
					if index == 0 {
						first = resolved
					} else if first == resolved {
						return false
					}
				}
				if previous != nil && previous != first {
					return false
				}
				previous = first
			}
		}
		return true
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 100, Rand: rand.New(rand.NewSource(23))}); err != nil {
		t.Fatal(err)
	}
}

func TestBindingsValidateScalarDeclarationsAndDefaults(t *testing.T) {
	spec := program.ProgramSpec{Params: []program.ParamSpec{{Name: "target", Kind: "Deployment", Required: true}, {Name: "replicas", ValueKind: program.IntValue, Required: true, Default: 2}}}
	b := program.Bindings{Objects: program.ObjectBindings{"target": {Kind: "Deployment"}}}
	if err := program.ValidateBindings(spec, b); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Parameter(spec, "replicas"); err != nil || v != 2 {
		t.Fatalf("default: %v %v", v, err)
	}
	b.Values = map[string]any{"replicas": 3}
	if v, err := b.Parameter(spec, "replicas"); err != nil || v != 3 {
		t.Fatalf("binding: %v %v", v, err)
	}
	for _, bad := range []program.Bindings{
		{Objects: b.Objects, Values: map[string]any{"replicas": "3"}},
		{Objects: b.Objects, Values: map[string]any{"target": "wrong"}},
		{Objects: b.Objects, Values: map[string]any{"unknown": 1}},
		{Objects: program.ObjectBindings{"target": {Kind: "Pod"}}},
	} {
		if err := program.ValidateBindings(spec, bad); err == nil {
			t.Fatalf("accepted invalid binding %+v", bad)
		}
	}
	for _, p := range []program.ParamSpec{
		{Name: "unknown", ValueKind: "unknown"},
		{Name: "both", Kind: "Deployment", ValueKind: program.IntValue},
		{Name: "bad-default", ValueKind: program.IntValue, Default: "2"},
	} {
		if err := program.ValidateBindings(program.ProgramSpec{Params: []program.ParamSpec{p}}, program.Bindings{}); err == nil {
			t.Fatalf("accepted invalid declaration %+v", p)
		}
	}
}

func TestSecretValueRefusedBeforeLookup(t *testing.T) {
	calls := 0
	read := func(sdk.ObjectRef) (map[string]any, error) { calls++; return nil, nil }
	for _, v := range []program.Value{program.FromSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "placeholder"}, "data[placeholder]"), program.FromObserved(sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "placeholder"}, "data[placeholder]")} {
		value, _, err := program.ResolveValue(v, program.ValueInputs{SourceCommit: "accepted", Source: read, Observed: read})
		if err == nil || value != nil {
			t.Fatal("Secret value resolved")
		}
	}
	if calls != 0 {
		t.Fatal("Secret reference reached object reader")
	}
}

func TestValueWireRejectsAmbiguousOrLiteralShape(t *testing.T) {
	for _, raw := range []string{`123`, `"literal"`, `null`, `{}`, `{"origin":"parameter","param":"count","path":"data[count]"}`, `{"origin":"parameter","param":"count","literal":3}`, `{"origin":"source","ref":{"api_version":"v1","kind":"ConfigMap","namespace":"app","name":"settings"},"path":"data[count]","param":"count"}`} {
		var value program.Value
		if err := json.Unmarshal([]byte(raw), &value); err == nil {
			t.Fatalf("accepted non-constructor value shape %s", raw)
		}
	}
}

type valueFirstProgram struct{}

func (valueFirstProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "value-first", Params: []program.ParamSpec{{Name: "replicas", ValueKind: program.IntValue, Required: true, Default: 2}, {Name: "service", Kind: "Service", Required: true}}}
}
func (valueFirstProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}
