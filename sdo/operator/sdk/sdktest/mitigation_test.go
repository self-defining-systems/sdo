package sdktest

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/targetport"
)

type fixtureMitigator struct{ actions []program.Action }

func (fixtureMitigator) Spec() program.ProgramSpec { return targetport.Program{}.Spec() }
func (m fixtureMitigator) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: m.actions}, nil
}
func TestMitigationMustRepairFaultedFixture(t *testing.T) {
	c := renameFixture(t)
	ref := c.Bindings["service"]
	for _, test := range []struct {
		name    string
		actions []program.Action
		want    string
	}{
		{"repairs port", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8080}}, ""},
		{"keeps faulty port", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 9999}}, "program.mitigation-not-repaired"},
		{"cannot simulate source", []program.Action{program.ReconcileFromSource{Ref: ref}}, "program.mitigation-unreplayable"},
		{"missing target", []program.Action{program.SetField{Ref: ref, Path: "spec.ports[99].targetPort", Value: 8080}}, "program.mitigation-unreplayable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := c.Open.Services()
			err := CheckMitigationRepairs(targetport.Program{}, fixtureMitigator{actions: test.actions}, c)
			if test.want == "" && err != nil || test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want)) {
				t.Fatalf("got %v, want %s", err, test.want)
			}
			if !reflect.DeepEqual(before, c.Open.Services()) {
				t.Fatal("replay mutated protected faulted fixture")
			}
		})
	}
}

func TestMitigationDoesNotReusePrimitiveAnswersAfterPatching(t *testing.T) {
	c := renameFixture(t)
	args, _ := json.Marshal([]any{c.Bindings["service"]})
	c.Recordings = []PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":true}`)}, {Phase: "fixed", Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{"changed":false}`)}}
	m := fixtureMitigator{actions: []program.Action{program.SetField{Ref: c.Bindings["service"], Path: "spec.ports[0].targetPort", Value: 8080}}}
	err := CheckMitigationRepairs(sourcePhaseProgram{}, m, c)
	if err == nil || !strings.Contains(err.Error(), "program.mitigation-unreplayable") {
		t.Fatalf("post-plan primitive effects were invented: %v", err)
	}
}
