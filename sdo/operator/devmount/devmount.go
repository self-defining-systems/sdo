// Package devmount projects a host-built operator and source tree into kind Pods.
package devmount

import (
	"os"

	corev1 "k8s.io/api/core/v1"
)

const NodePath = "/opt/sdo-dev"

// Enabled is set only for a fastloop lane whose kind nodes mount the worktree.
func Enabled() bool { return os.Getenv("SDO_DEV_MOUNT") == "1" }

// Apply gives every container the same read-only source and operator binary.
func Apply(pod *corev1.PodSpec) {
	directory, file := corev1.HostPathDirectory, corev1.HostPathFile
	for _, name := range []string{"sdo", "libs", "examples"} {
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name:         "dev-" + name,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: NodePath + "/" + name, Type: &directory}},
		})
	}
	pod.Volumes = append(pod.Volumes, corev1.Volume{
		Name:         "dev-operator",
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: NodePath + "/.fastloop/sdo-operator", Type: &file}},
	})
	if os.Getenv("SDO_DEV_MODULE_DOWNLOADS") == "1" {
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name: "dev-module-downloads",
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: NodePath + "/.fastloop/go-mod-download", Type: &directory,
			}},
		})
	}
	for index := range pod.InitContainers {
		mount(&pod.InitContainers[index])
	}
	for index := range pod.Containers {
		mount(&pod.Containers[index])
	}
}

func mount(container *corev1.Container) {
	if os.Getenv("SDO_DEV_MODULE_DOWNLOADS") == "1" {
		container.VolumeMounts = append(container.VolumeMounts,
			corev1.VolumeMount{Name: "dev-module-downloads", MountPath: "/go/pkg/mod/cache/download", ReadOnly: true})
		container.Env = append(container.Env, corev1.EnvVar{Name: "SDO_DEV_MODULE_DOWNLOADS", Value: "1"})
	}
	for _, name := range []string{"sdo", "libs", "examples"} {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name: "dev-" + name, MountPath: "/opt/sdo/" + name, ReadOnly: true,
		})
	}
	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{Name: "dev-operator", MountPath: "/usr/local/bin/sdo-operator", ReadOnly: true})
}
