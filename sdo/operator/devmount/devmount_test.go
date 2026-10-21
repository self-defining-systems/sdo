package devmount

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestApplyMountsSourcePackagesAndHostBinaryReadOnly(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	Apply(pod)
	if len(pod.Volumes) != 4 || len(pod.Containers[0].VolumeMounts) != 4 {
		t.Fatalf("dev volumes=%v mounts=%v", pod.Volumes, pod.Containers[0].VolumeMounts)
	}
	paths := map[string]string{
		"/opt/sdo/sdo":                "/opt/sdo-dev/sdo",
		"/opt/sdo/benchmarks":         "/opt/sdo-dev/benchmarks",
		"/opt/sdo/libs":               "/opt/sdo-dev/libs",
		"/usr/local/bin/sdo-operator": "/opt/sdo-dev/.fastloop/sdo-operator",
	}
	for index, mount := range pod.Containers[0].VolumeMounts {
		if !mount.ReadOnly {
			t.Fatalf("writable dev mount: %+v", mount)
		}
		if paths[mount.MountPath] != pod.Volumes[index].HostPath.Path {
			t.Fatalf("dev mount=%+v volume=%+v", mount, pod.Volumes[index])
		}
	}
}
