package targetport

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sdo.dev/operator/sdk/program"
)

// Program checks whether a Service targetPort names a port on a selected pod.
type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	ref := b["service"]
	for _, service := range v.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		pods := v.PodsForService(ref.Namespace, ref.Name)
		if len(pods) == 0 {
			return program.Verdict{Result: program.Unknown}, nil
		}
		for _, port := range service.Spec.Ports {
			target := port.TargetPort
			if target == (intstr.IntOrString{}) {
				target = intstr.FromInt32(port.Port)
			}
			for _, pod := range pods {
				found := false
				for _, container := range pod.Spec.Containers {
					for _, exposed := range container.Ports {
						if target.Type == intstr.Int && exposed.ContainerPort == target.IntVal || target.Type == intstr.String && exposed.Name == target.StrVal {
							found = true
						}
					}
				}
				if !found {
					return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "targetPort on selected pod", Observed: fmt.Sprint(target), Expected: "declared containerPort"}}}, nil
				}
			}
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Unknown}, nil
}
