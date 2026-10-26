// Package devmount projects a host-built operator and source tree into kind Pods.
package devmount

import (
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
)

const NodePath = "/opt/sdo-dev"

// Enabled is set only for a fastloop lane whose kind nodes mount the worktree.
func Enabled() bool { return os.Getenv("SDO_DEV_MOUNT") == "1" }

// benchmarkFiles is an allowlist: mounting all of benchmarks would expose the
// fault catalog and assurance answer keys to live model workers.
var benchmarkFiles = []string{
	"benchmarks/__init__.py", "benchmarks/sregym/__init__.py",
	"benchmarks/sregym/adapter/__init__.py", "benchmarks/sregym/adapter/submission.py",
	"benchmarks/sregym/adapter/submission_relay.py", "benchmarks/sregym/adapter/scripted_worker.py",
	"benchmarks/sregym/protocol/__init__.py", "benchmarks/sregym/protocol/_http.py",
	"benchmarks/sregym/protocol/benchmark.py", "benchmarks/sregym/protocol/conductor.py",
	"benchmarks/sregym/protocol/mcp_client.py", "benchmarks/sregym/protocol/receipt.py",
	"benchmarks/sregym/protocol/schema.py", "benchmarks/sregym/protocol/submission_guard.py",
	"benchmarks/sregym/fastloop/__init__.py", "benchmarks/sregym/fastloop/model_recording.py",
	"benchmarks/sregym/fastloop/file_edits.py", "benchmarks/sregym/fastloop/recording_worker.py",
}

// workerDirectories expose only reusable scripted fixtures, never fault catalogs.
var workerDirectories = []struct{ source, target string }{
	{"benchmarks/sregym/fastloop/scripted_program", "/opt/sdo-scripted/targetport"},
	{"benchmarks/sregym/fastloop/scripted_networkpolicy_program", "/opt/sdo-scripted/networkpolicy"},
	{"apps/toy/seed", "/opt/sdo/apps/toy/seed"},
	{"apps/toy/kubernetes", "/opt/sdo/apps/toy/kubernetes"},
}

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

// ApplyWorker adds only the benchmark worker entry points to a development
// agent Job. Controllers and validators remain free of benchmark code.
func ApplyWorker(pod *corev1.PodSpec) {
	Apply(pod)
	directory, file := corev1.HostPathDirectory, corev1.HostPathFile
	for index, path := range benchmarkFiles {
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: fmt.Sprintf("dev-bench-%d", index), VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: NodePath + "/" + path, Type: &file}}})
	}
	for index, directoryMount := range workerDirectories {
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: fmt.Sprintf("dev-script-%d", index), VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: NodePath + "/" + directoryMount.source, Type: &directory}}})
	}

	for index := range pod.Containers {
		mountWorker(&pod.Containers[index])
	}
	for index := range pod.InitContainers {
		mountWorker(&pod.InitContainers[index])
	}
}

func mountWorker(container *corev1.Container) {
	for index, path := range benchmarkFiles {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: fmt.Sprintf("dev-bench-%d", index), MountPath: "/opt/sdo/" + path, ReadOnly: true})
	}
	for index, directoryMount := range workerDirectories {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: fmt.Sprintf("dev-script-%d", index), MountPath: directoryMount.target, ReadOnly: true})
	}

}
