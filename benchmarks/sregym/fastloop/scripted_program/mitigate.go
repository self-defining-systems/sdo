package service_targetport_mismatch

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	ref := bindings.Objects["service"]
	for _, service := range view.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		for index, port := range service.Spec.Ports {
			for _, pod := range view.PodsForService(ref.Namespace, ref.Name) {
				var replacement int32
				var replacementValue program.Value
				matched := false
				for ci, container := range pod.Spec.Containers {
					for pi, exposed := range container.Ports {
						if replacement == 0 {
							replacement = exposed.ContainerPort
							replacementValue = program.FromObserved(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, fmt.Sprintf("spec.containers[%d].ports[%d].containerPort", ci, pi))
						}
						matched = matched || port.TargetPort.Type == intstr.Int && port.TargetPort.IntVal == exposed.ContainerPort || port.TargetPort.Type == intstr.String && port.TargetPort.StrVal == exposed.Name
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
