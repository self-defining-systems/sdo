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
