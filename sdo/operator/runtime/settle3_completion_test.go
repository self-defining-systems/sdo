package runtime

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type settle3ObservedProgram struct {
	client *externalTestClient
	ref    sdk.ObjectRef
}

func (p settle3ObservedProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "synthetic-observed", Params: []program.ParamSpec{{Name: "target", Kind: "Pod", Required: true}}}
}

func (p settle3ObservedProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	value, _, _ := unstructured.NestedString(p.client.objects[p.ref].Object, "metadata", "labels", "ready")
	if value == "yes" {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match}, nil
}

func (p settle3ObservedProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{
		program.PatchObject{Ref: p.ref, Type: "merge", Patch: json.RawMessage(`{"metadata":{"labels":{"ready":"yes"}}}`)},
		program.EnsureExec{Observe: program.Command{Pod: p.ref, Container: "db", Args: []string{"observe"}}, Ensure: program.Command{Pod: p.ref, Container: "db", Args: []string{"ensure"}}},
		program.WaitFor{Ref: p.ref, Path: "/metadata/labels/ready", Equals: "yes"},
	}}, nil
}
