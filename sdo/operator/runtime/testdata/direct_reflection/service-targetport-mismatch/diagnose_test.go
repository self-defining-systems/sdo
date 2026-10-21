package targetport

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestObservedTargetPortMismatchAndNearMiss(t *testing.T) {
	pod := corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 8087}}}}}}
	for _, tc := range []struct {
		name   string
		target int32
		want   program.Result
	}{{"observed 9999 mismatch", 9999, program.Match}, {"near miss matching declared port", 8087, program.NoMatch}} {
		t.Run(tc.name, func(t *testing.T) {
			matched := declaresTarget(pod, intstr.FromInt32(tc.target))
			got := program.NoMatch
			if !matched {
				got = program.Match
			}
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
