// Package toolchain supplies isolated Jobs with the controller image's compiler.
// Only the controller image stores Go; the receiving image stays compiler-free.
package toolchain

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Apply copies the trusted toolchain into a private emptyDir before untrusted
// code starts. The init container sees neither the candidate nor credentials;
// the worker sees the compiler read-only and writes its build cache to scratch.
func Apply(pod *corev1.PodSpec, controllerImage string) {
	if controllerImage == "" {
		return
	} // Older/custom registrations may carry their own compiler.
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "sdo-toolchain", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	no, yes := false, true
	pod.InitContainers = append(pod.InitContainers, corev1.Container{
		Name: "prepare-toolchain", Image: controllerImage, ImagePullPolicy: corev1.PullIfNotPresent,
		TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		Command:         []string{"/bin/sh", "-ec", "cp -R /usr/local/go /sdo-toolchain/go; cp -R /go /sdo-toolchain/cache"},
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
		Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("256Mi")}},
		VolumeMounts:    []corev1.VolumeMount{{Name: "sdo-toolchain", MountPath: "/sdo-toolchain"}},
	})
	for i := range pod.Containers {
		pod.Containers[i].VolumeMounts = append(pod.Containers[i].VolumeMounts,
			corev1.VolumeMount{Name: "sdo-toolchain", MountPath: "/usr/local/go", SubPath: "go", ReadOnly: true},
			corev1.VolumeMount{Name: "sdo-toolchain", MountPath: "/go", SubPath: "cache", ReadOnly: true})
	}
}
