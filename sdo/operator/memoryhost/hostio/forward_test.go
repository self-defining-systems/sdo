package hostio

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestTargetPortResolvesNamedNumberedAndDefaultPorts(t *testing.T) {
	pod := corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}}}}
	for _, c := range []struct {
		port corev1.ServicePort
		want int32
		ok   bool
	}{
		{corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("http")}, 8080, true},
		{corev1.ServicePort{Port: 80, TargetPort: intstr.FromInt32(9090)}, 9090, true},
		{corev1.ServicePort{Port: 80}, 80, true},
		{corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("absent")}, 0, false},
		{corev1.ServicePort{Port: 81}, 0, false},
	} {
		service := corev1.Service{Spec: corev1.ServiceSpec{Ports: []corev1.ServicePort{c.port}}}
		if got, ok := targetPort(service, 80, pod); got != c.want || ok != c.ok {
			t.Errorf("%+v: got %d %v", c.port, got, ok)
		}
	}
}
