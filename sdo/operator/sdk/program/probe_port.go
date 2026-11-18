package program

import corev1 "k8s.io/api/core/v1"

// HTTPProbePort is the Service port ProbeHTTP dials: the first TCP port in
// declaration order. HTTP rides TCP, so a leading UDP port (a tracing agent's,
// say) is skipped; ok is false when the Service declares no TCP port.
func HTTPProbePort(service corev1.Service) (port int32, ok bool) {
	for _, p := range service.Spec.Ports {
		if p.Protocol == "" || p.Protocol == corev1.ProtocolTCP {
			return p.Port, true
		}
	}
	return 0, false
}
