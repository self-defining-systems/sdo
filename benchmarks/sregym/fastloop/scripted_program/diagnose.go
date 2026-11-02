package service_targetport_mismatch

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	ref := bindings.Objects["service"]
	for _, service := range view.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		pods := view.PodsForService(ref.Namespace, ref.Name)
		if len(pods) == 0 {
			return program.Verdict{Result: program.Unknown}, nil
		}
		for _, port := range service.Spec.Ports {
			target := port.TargetPort
			if target == (intstr.IntOrString{}) {
				target = intstr.FromInt32(port.Port)
			}
			for _, pod := range pods {
				matched := false
				for _, container := range pod.Spec.Containers {
					for _, exposed := range container.Ports {
						matched = matched || target.Type == intstr.Int && target.IntVal == exposed.ContainerPort || target.Type == intstr.String && target.StrVal == exposed.Name
					}
				}
				if !matched {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "targetPort on selected pod", Observed: fmt.Sprint(target), Expected: "declared containerPort"}}}, nil
				}
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Unknown}, nil
}
