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

func TestBindFindingOptionalPeerDoesNotObscureRequiredRole(t *testing.T) {
	spec := program.ProgramSpec{ID: "optional-peer", Params: []program.ParamSpec{{Name: "primary", Kind: "Service", Required: true}, {Name: "peer", Kind: "Service"}}}
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}
	got, err := program.BindFinding(spec, sdk.Finding{PrimaryResource: ref})
	if err != nil || got.Objects["primary"] != ref || len(got.Objects) != 1 {
		t.Fatalf("optional peer obscured the unique required binding: %+v %v", got, err)
	}
}

// The shared-memory hotel repeat (PAR3, h9.18c): one lane's reflection saved
// health-objective with a required "deployment" role; a peer's reflection
// added an optional "resource" alias and made both optional. The generated
// health-objective detector emits unnamed findings on the unhealthy
// Deployment, so the stage bound neither role and every diagnosis returned
// NO_MATCH with empty bindings.
func TestCheckFindingRolesRefusesAliasRolesOfOneKind(t *testing.T) {
	alias := program.ProgramSpec{ID: "health-objective", Params: []program.ParamSpec{{Name: "resource", Kind: "Deployment"}, {Name: "deployment", Kind: "Deployment"}}}
	finding := sdk.Finding{DetectorID: "health-objective", PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel-reservation", Name: "recommendation"}}
	if got, err := program.BindFinding(alias, finding); err != nil || len(got.Objects) != 0 {
		t.Fatalf("alias roles: want the empty binding the repeat recorded, got %+v %v", got, err)
	}
	if err := program.CheckFindingRoles(alias); err == nil || !strings.Contains(err.Error(), "program.finding-bindings") || !strings.Contains(err.Error(), "one role per kind") {
		t.Fatalf("alias roles accepted: %v", err)
	}
	single := program.ProgramSpec{ID: "health-objective", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
	if err := program.CheckFindingRoles(single); err != nil {
		t.Fatalf("one role per kind refused: %v", err)
	}
	if got, err := program.BindFinding(single, finding); err != nil || got.Objects["deployment"] != finding.PrimaryResource {
		t.Fatalf("one role per kind did not bind the health finding: %+v %v", got, err)
	}
}

func TestCheckFindingRolesKeepsNamedPairsAndOptionalPeers(t *testing.T) {
	for _, spec := range []program.ProgramSpec{
		{ID: "pair", Params: []program.ParamSpec{{Name: "from", Kind: "Deployment", Required: true}, {Name: "to", Kind: "Deployment", Required: true}}},
		{ID: "optional-peer", Params: []program.ParamSpec{{Name: "primary", Kind: "Service", Required: true}, {Name: "peer", Kind: "Service"}}},
		{ID: "mixed", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "peer", Kind: "Deployment"}, {Name: "port", ValueKind: program.IntValue, Required: true, Default: 8080}}},
		{ID: "values-only", Params: []program.ParamSpec{{Name: "port", ValueKind: program.IntValue, Default: 8080}}},
	} {
		if err := program.CheckFindingRoles(spec); err != nil {
			t.Errorf("%s: %v", spec.ID, err)
		}
	}
	mixedAlias := program.ProgramSpec{ID: "mixed-alias", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "a", Kind: "Deployment"}, {Name: "b", Kind: "Deployment"}}}
	if err := program.CheckFindingRoles(mixedAlias); err == nil {
		t.Fatal("optional aliases next to a required role accepted")
	}
}
