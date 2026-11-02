package repair

import (
	"context"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "extra-workload-listener", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	for _, d := range v.Deployments() {
		if d.Namespace == ref.Namespace && d.Name == ref.Name {
			if len(d.Spec.Template.Spec.Containers) > 1 {
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "unexpected workload listener", Observed: "present", Expected: "absent"}}}, nil
			}
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}
