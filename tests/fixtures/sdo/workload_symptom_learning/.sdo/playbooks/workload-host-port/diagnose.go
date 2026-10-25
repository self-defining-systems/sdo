package repair

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "workload-host-port", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b.Objects["deployment"]
	for _, d := range v.Deployments() {
		if d.Namespace == ref.Namespace && d.Name == ref.Name {
			if hasHostPort(d.Spec.Template.Spec.Containers) {
				return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "unexpected workload listener", Observed: "present", Expected: "absent"}}}, nil
			}
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Unknown}, nil
}

func hasHostPort(containers []corev1.Container) bool {
	for _, c := range containers {
		for _, p := range c.Ports {
			if p.HostPort != 0 {
				return true
			}
		}
	}
	return false
}
