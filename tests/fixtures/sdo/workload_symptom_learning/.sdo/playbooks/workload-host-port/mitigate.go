package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["deployment"]
	for _, d := range v.Deployments() {
		if d.Namespace != ref.Namespace || d.Name != ref.Name {
			continue
		}
		for ci, c := range d.Spec.Template.Spec.Containers {
			for pi, p := range c.Ports {
				if p.HostPort != 0 {
					patch := json.RawMessage(fmt.Sprintf(`[{"op":"remove","path":"/spec/template/spec/containers/%d/ports/%d/hostPort"}]`, ci, pi))
					return program.Plan{Actions: []program.Action{program.PatchObject{Ref: ref, Type: "json", Patch: patch, Reason: "remove conflicting host listener"}}}, nil
				}
			}
		}
	}
	return program.Plan{}, nil
}
