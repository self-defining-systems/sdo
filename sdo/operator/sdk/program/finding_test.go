package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

func TestBindFindingDoesNotEnumerateAmbiguousRoles(t *testing.T) {
	a := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "a"}
	b := sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "b"}
	spec := program.ProgramSpec{ID: "pair", Params: []program.ParamSpec{{Name: "from", Kind: "Deployment", Required: true}, {Name: "to", Kind: "Deployment", Required: true}}}
	finding := sdk.Finding{PrimaryResource: a, RelatedResources: []sdk.ObjectRef{b}}
	if _, err := program.BindFinding(spec, finding); err == nil || !strings.Contains(err.Error(), "program.finding-bindings") || !strings.Contains(err.Error(), "ParameterBindings") {
		t.Fatalf("ambiguous required roles accepted: %v", err)
	}
	finding.ParameterBindings = map[string]sdk.ObjectRef{"from": a, "to": b}
	got, err := program.BindFinding(spec, finding)
	if err != nil || got.Objects["from"] != a || got.Objects["to"] != b {
		t.Fatalf("explicit role bindings: %+v %v", got, err)
	}
}
func TestBindFindingUniqueResourceAndDefaults(t *testing.T) {
	spec := program.ProgramSpec{ID: "service", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "peer", Kind: "Deployment"}, {Name: "port", ValueKind: program.IntValue, Required: true, Default: 8080}}}
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	got, err := program.BindFinding(spec, sdk.Finding{PrimaryResource: ref, RelatedResources: []sdk.ObjectRef{ref}})
	if err != nil || len(got.Objects) != 1 || got.Objects["service"] != ref {
		t.Fatalf("unique resource: %+v %v", got, err)
	}
}
func TestBindFindingExplicitRolesWinAndRejectWrongKind(t *testing.T) {
	spec := program.ProgramSpec{ID: "service", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "bound"}
	f := sdk.Finding{PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "other"}, ParameterBindings: map[string]sdk.ObjectRef{"service": ref}}
	got, err := program.BindFinding(spec, f)
	if err != nil || got.Objects["service"] != ref {
		t.Fatalf("explicit role lost: %+v %v", got, err)
	}
	f.ParameterBindings["service"] = sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "wrong"}
	if _, err := program.BindFinding(spec, f); err == nil {
		t.Fatal("wrong explicit kind fell back to primary")
	}
}
