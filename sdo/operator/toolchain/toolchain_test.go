package toolchain

import (
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestControllerSuppliesReadOnlyCompilerWithoutCredentials(t *testing.T) {
	pod := corev1.PodSpec{Containers: []corev1.Container{{Name: "validator", Image: "validator:slim"}}}
	Apply(&pod, "controller:tools")
	if len(pod.InitContainers) != 1 || pod.InitContainers[0].Image != "controller:tools" {
		t.Fatalf("compiler init: %+v", pod.InitContainers)
	}
	init := pod.InitContainers[0]
	if len(init.EnvFrom) != 0 || len(init.VolumeMounts) != 1 || init.VolumeMounts[0].Name != "sdo-toolchain" {
		t.Fatalf("compiler can see candidate or credentials: %+v", init)
	}
	if len(pod.Containers[0].VolumeMounts) != 2 {
		t.Fatalf("missing compiler mounts: %+v", pod)
	}
	for _, mount := range pod.Containers[0].VolumeMounts {
		if !mount.ReadOnly {
			t.Fatalf("writable compiler: %+v", mount)
		}
	}
}

func TestCompilerInitPinsAdmissionTerminationDefaults(t *testing.T) {
	pod := corev1.PodSpec{Containers: []corev1.Container{{Name: "validator"}}}
	Apply(&pod, "controller:compiler")
	init := pod.InitContainers[0]
	if init.TerminationMessagePath != "/dev/termination-log" || init.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("validator admission would change compiler init: %+v", init)
	}
}
