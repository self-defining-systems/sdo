package targetport

import (
	"context"
	"fmt"

	"fixture-diagnostics/lib/ports"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["service"]
	for _, service := range v.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		for index, port := range service.Spec.Ports {
			for _, pod := range v.PodsForService(ref.Namespace, ref.Name) {
				var replacement int32
				var replacementValue program.Value
				matched := false
				for ci, container := range pod.Spec.Containers {
					for pi, exposed := range container.Ports {
						if replacement == 0 {
							replacement = exposed.ContainerPort
							replacementValue = program.FromObserved(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, fmt.Sprintf("spec.containers[%d].ports[%d].containerPort", ci, pi))
						}
						matched = matched || ports.Matches(port.TargetPort, exposed)
					}
				}
				if !matched && replacement > 0 {
					return program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: fmt.Sprintf("spec.ports[%d].targetPort", index), Value: replacementValue, Reason: "use a selected pod port"}}}, nil
				}
			}
		}
	}
	return program.Plan{}, nil
}
