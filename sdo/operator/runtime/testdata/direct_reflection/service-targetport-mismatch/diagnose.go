package targetport

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (Program) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	ref := bindings["service"]
	for _, service := range view.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		pods := view.PodsForService(ref.Namespace, ref.Name)
		if len(pods) == 0 {
			return program.Verdict{Result: program.Unknown}, nil
		}
		for index, port := range service.Spec.Ports {
			target := port.TargetPort
			if target == (intstr.IntOrString{}) {
				target = intstr.FromInt32(port.Port)
			}
			for _, pod := range pods {
				if !declaresTarget(pod, target) {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: fmt.Sprintf("spec.ports[%d].targetPort is not declared by a selected pod", index), Observed: target.String(), Expected: "a selected pod containerPort name or number"}}}, nil
				}
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Unknown}, nil
}

func declaresTarget(pod corev1.Pod, target intstr.IntOrString) bool {
	for _, container := range pod.Spec.Containers {
		for _, port := range container.Ports {
			if target.Type == intstr.Int && port.ContainerPort == target.IntVal || target.Type == intstr.String && port.Name == target.StrVal {
				return true
			}
		}
	}
	return false
}
