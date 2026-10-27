package program

import (
	"context"
	"testing"
)

type registeredProgram struct{ id string }

func (p registeredProgram) Spec() ProgramSpec { return ProgramSpec{ID: p.id} }
func (p registeredProgram) Diagnose(context.Context, ReadView, Bindings) (Verdict, error) {
	return Verdict{Result: NoMatch}, nil
}
func (p registeredProgram) Mitigate(context.Context, ReadView, Bindings) (Plan, error) {
	return Plan{}, nil
}

func TestRegistryExposesCompiledProgramsByLogicalID(t *testing.T) {
	item := registeredProgram{"targetport"}
	registry, err := NewRegistry([]Diagnoser{item}, []Mitigator{item})
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.List(); len(got) != 1 || got[0].ID != "targetport" {
		t.Fatalf("specs: %+v", got)
	}
	if _, ok := registry.Diagnoser("targetport"); !ok {
		t.Fatal("missing diagnoser")
	}
	if _, ok := registry.Mitigator("targetport"); !ok {
		t.Fatal("missing mitigator")
	}
	if _, ok := registry.Diagnoser("unknown"); ok {
		t.Fatal("unknown diagnoser found")
	}
}

func TestRegistryRejectsDuplicatePrograms(t *testing.T) {
	item := registeredProgram{"targetport"}
	if _, err := NewRegistry([]Diagnoser{item, item}, nil); err == nil {
		t.Fatal("duplicate accepted")
	}
}

type declaredProgram struct {
	registeredProgram
	spec ProgramSpec
}

func (p declaredProgram) Spec() ProgramSpec { return p.spec }

func TestRegistryRejectsParametersWithoutLiveBindingPath(t *testing.T) {
	for _, kind := range []ValueKind{StringValue, IntValue, QuantityValue, DurationValue, BoolValue} {
		p := declaredProgram{spec: ProgramSpec{ID: "repair", Params: []ParamSpec{{Name: "recorded-only", ValueKind: kind, Required: true}}}}
		if _, err := NewRegistry([]Diagnoser{p}, []Mitigator{p}); err == nil {
			t.Fatalf("accepted required %s parameter with no live finding/default binding", kind)
		}
	}
}
func TestRegistryRequiresSameDiagnosisAndMitigationDeclaration(t *testing.T) {
	diagnosis := declaredProgram{spec: ProgramSpec{ID: "repair", Params: []ParamSpec{{Name: "resource", Kind: "Service", Required: true}}}}
	mitigation := declaredProgram{spec: ProgramSpec{ID: "repair", Params: []ParamSpec{{Name: "recorded-only", Kind: "Deployment", Required: true}}}}
	if _, err := NewRegistry([]Diagnoser{diagnosis}, []Mitigator{mitigation}); err == nil {
		t.Fatal("accepted a mitigation with parameters not supplied to Diagnose")
	}
}
