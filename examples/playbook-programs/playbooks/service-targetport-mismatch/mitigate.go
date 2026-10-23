package routingport

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, v, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, err
	}
	_, pods, port, ok := replacement(v, b)
	if !ok {
		return program.Plan{}, nil
	}
	for _, pod := range pods[:1] {
		for ci, container := range pod.Spec.Containers {
			for pi, exposed := range container.Ports {
				if exposed.ContainerPort == port && (exposed.Protocol == "" || exposed.Protocol == "TCP") {
					value := program.FromObserved(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, fmt.Sprintf("spec.containers[%d].ports[%d].containerPort", ci, pi))
					return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: value, Reason: "use observed selected-Pod listener"}}}, nil
				}
			}
		}
	}
	return program.Plan{}, nil
}
