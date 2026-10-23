package program_test

import (
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"testing/quick"
	"time"
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

func TestSetFieldLiteralDoesNotCompile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "go", "build", "./testdata/literal").CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	if err == nil || !strings.Contains(string(output), "cannot use 8080") || !strings.Contains(string(output), "program.Value") {
		t.Fatalf("expected typed literal compile failure, got %v: %s", err, output)
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
