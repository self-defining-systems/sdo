package runtime

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type roleSweepProgram struct{ spec program.ProgramSpec }

func (p roleSweepProgram) Spec() program.ProgramSpec { return p.spec }
func (roleSweepProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.NoMatch}, nil
}

// The save-time role gate and the incident stage agree for every small role
// shape: a declaration passes program.CheckFindingRoles exactly when the stage
// binds a role of each declared kind from the unnamed finding a health or
// generated detector emits on one object of that kind (kinds that need named
// bindings excepted). The shared hotel repeat ran 70 unbound diagnoses because
// alias roles passed save time.
func TestStageBindsUnnamedFindingsForEveryGateAcceptedRoleShape(t *testing.T) {
	kinds := []program.Kind{"Deployment", "Service"}
	var shapes [][]program.ParamSpec
	var grow func([]program.ParamSpec)
	grow = func(params []program.ParamSpec) {
		if len(params) > 0 {
			shapes = append(shapes, params)
		}
		if len(params) == 3 {
			return
		}
		for _, kind := range kinds {
			for _, required := range []bool{true, false} {
				next := append(append([]program.ParamSpec(nil), params...), program.ParamSpec{Name: fmt.Sprintf("r%d", len(params)), Kind: kind, Required: required})
				grow(next)
			}
		}
	}
	grow(nil)
	accepted, refused := 0, 0
	for index, params := range shapes {
		spec := program.ProgramSpec{ID: fmt.Sprintf("shape-%d", index), Params: params}
		registry, err := program.NewRegistry([]program.Diagnoser{roleSweepProgram{spec}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		unbound := false
		for _, kind := range kinds {
			declared := false
			for _, p := range params {
				declared = declared || p.Kind == kind
			}
			if !declared {
				continue
			}
			finding := sdk.Finding{DetectorID: "health-objective", Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "apps/v1", Kind: string(kind), Namespace: "app", Name: "unhealthy"}}
			for _, other := range kinds {
				if other != kind {
					finding.RelatedResources = append(finding.RelatedResources, sdk.ObjectRef{Kind: string(other), Namespace: "app", Name: "related"})
				}
			}
			if _, err := program.BindFinding(spec, finding); err != nil {
				continue
			}
			runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{finding}, time.Second)
			filled := false
			for _, run := range runs {
				for _, p := range params {
					filled = filled || (p.Kind == kind && run.Bindings.Objects[p.Name] == finding.PrimaryResource && run.BindingSource == "finding")
				}
			}
			unbound = unbound || !filled
		}
		gate := program.CheckFindingRoles(spec)
		if (gate == nil) == unbound {
			t.Errorf("%v: gate=%v but stage left a kind unbound=%v", params, gate, unbound)
		}
		if gate == nil {
			accepted++
		} else {
			refused++
		}
	}
	if accepted == 0 || refused == 0 {
		t.Fatalf("sweep must cover both outcomes: accepted=%d refused=%d", accepted, refused)
	}
}
