// Package routingport demonstrates targetPort repair from observed Pod listeners.
package routingport

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

// replacement is deliberately narrow: one TCP Service port and one common
// declared TCP port on every selected ready Pod. Declarations choose probes;
// they never establish a listener without a successful TCP observation.
func replacement(v program.ReadView, b program.Bindings) (corev1.Service, []corev1.Pod, int32, bool) {
	ref := b.Objects["service"]
	for _, service := range v.Services() {
		if service.Namespace != ref.Namespace || service.Name != ref.Name {
			continue
		}
		if len(service.Spec.Ports) != 1 || service.Spec.Type == corev1.ServiceTypeExternalName {
			return service, nil, 0, false
		}
		if proto := service.Spec.Ports[0].Protocol; proto != "" && proto != corev1.ProtocolTCP {
			return service, nil, 0, false
		}
		pods := v.PodsForService(ref.Namespace, ref.Name)
		if len(pods) == 0 {
			return service, nil, 0, false
		}
		var common int32
		for _, pod := range pods {
			ready := false
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
					ready = true
				}
			}
			if !ready {
				return service, nil, 0, false
			}
			ports := map[int32]bool{}
			for _, container := range pod.Spec.Containers {
				for _, port := range container.Ports {
					if port.Protocol == "" || port.Protocol == corev1.ProtocolTCP {
						ports[port.ContainerPort] = true
					}
				}
			}
			if len(ports) != 1 {
				return service, nil, 0, false
			}
			for port := range ports {
				if port < 1 || port > 65535 || common != 0 && common != port {
					return service, nil, 0, false
				}
				common = port
			}
		}
		return service, pods, common, true
	}
	return corev1.Service{}, nil, 0, false
}
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	service, pods, port, ok := replacement(v, b)
	if !ok {
		return program.Verdict{Result: program.Unknown}, nil
	}
	target := service.Spec.Ports[0].TargetPort
	if target.Type != intstr.Int {
		return program.Verdict{Result: program.Unknown}, nil
	}
	targetPort := target.IntVal
	if targetPort == 0 {
		targetPort = service.Spec.Ports[0].Port
	}
	for _, pod := range pods {
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
		listener, err := v.ProbeTCP(ref, port)
		if err != nil {
			return program.Verdict{}, err
		}
		if !listener.Success {
			return program.Verdict{Result: program.Unknown}, nil
		}
		if targetPort != port {
			wrong, err := v.ProbeTCP(ref, targetPort)
			if err != nil {
				return program.Verdict{}, err
			}
			if wrong.Success {
				return program.Verdict{Result: program.Unknown}, nil
			}
		}
	}
	if targetPort == port {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: b.Objects["service"], Fact: "selected ready Pods accept declared port and refuse numeric targetPort", Observed: fmt.Sprint(targetPort), Expected: fmt.Sprint(port)}}}, nil
}
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
