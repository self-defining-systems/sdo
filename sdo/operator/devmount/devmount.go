// Package devmount projects a host-built operator and source tree into kind Pods.
package devmount

import (
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
)

// DefaultNodePath is where a fastloop lane's kind nodes mount the worktree.
const DefaultNodePath = "/opt/sdo-dev"

// NodePath is the node directory holding the projected sources: SDO_DEV_MOUNT_PATH when set (a fuzzops lane keeps
// them under its evidence mount, which every node already has), else DefaultNodePath.
func NodePath() string {
	if path := os.Getenv("SDO_DEV_MOUNT_PATH"); path != "" {
		return path
	}
	return DefaultNodePath
}

// VolumeNodeAnnotations binds a claim of a lane on a shared multi-node cluster to SDO_VOLUME_NODE, where the lane's other
// claims are: a node-local volume (local-path) otherwise binds to the first consumer's node, and a pod that mounts two
// claims is unschedulable when they differ. Nil when the variable is unset.
func VolumeNodeAnnotations() map[string]string {
	if node := os.Getenv("SDO_VOLUME_NODE"); node != "" {
		return map[string]string{"volume.kubernetes.io/selected-node": node}
	}
	return nil
}

// Claim is the PersistentVolumeClaim that holds the projected sources when the lane has no node mount (a lane on a
// shared multi-node cluster): SDO_DEV_MOUNT_CLAIM. Then NodePath is the directory inside the claim and every mount
// is a subPath of it. Empty for a node directory.
func Claim() string { return os.Getenv("SDO_DEV_MOUNT_CLAIM") }

// claimVolume is the one volume of a claim-backed pod.
const claimVolume = "dev-tree"

// Environment is what the controller container needs to launch dev-mounted workers like itself.
func Environment() []corev1.EnvVar {
	env := []corev1.EnvVar{{Name: "SDO_DEV_MOUNT", Value: "1"}}
	if path := os.Getenv("SDO_DEV_MOUNT_PATH"); path != "" {
		env = append(env, corev1.EnvVar{Name: "SDO_DEV_MOUNT_PATH", Value: path})
	}
	if claim := Claim(); claim != "" {
		env = append(env, corev1.EnvVar{Name: "SDO_DEV_MOUNT_CLAIM", Value: claim})
	}
	return env
}

// Enabled is set only for a fastloop lane whose kind nodes mount the worktree.
func Enabled() bool { return os.Getenv("SDO_DEV_MOUNT") == "1" }

// benchmarkFiles is an allowlist: mounting all of benchmarks would expose the
// fault catalog and assurance answer keys to live model workers.
var benchmarkFiles = []string{
	"benchmarks/sregym/fastloop/generated_worker.py",
	"benchmarks/sregym/fastloop/responder_script.py",
	"benchmarks/sregym/fastloop/responder_steps.py",
	"benchmarks/sregym/fastloop/reflection_script.py",
	"benchmarks/sregym/fastloop/judge_script.py",
	"benchmarks/sregym/agent_script/template.py",
	"benchmarks/sregym/agent_script/model.py",
	"benchmarks/sregym/agent_script/invocation.py",
	"benchmarks/sregym/agent_script/__init__.py",
	"benchmarks/sregym/agent_script/judge_example.py",
	"benchmarks/sregym/agent_script/runner.py",
	"benchmarks/sregym/agent_script/converter.py",
	"benchmarks/sregym/agent_script/cli.py",
	"benchmarks/sregym/agent_script/wire.py",
	"benchmarks/sregym/agent_script/__main__.py",

	"benchmarks/__init__.py", "benchmarks/sregym/__init__.py",
	"benchmarks/sregym/adapter/__init__.py", "benchmarks/sregym/adapter/submission.py",
	"benchmarks/sregym/adapter/submission_relay.py",
	"benchmarks/sregym/protocol/__init__.py", "benchmarks/sregym/protocol/_http.py",
	"benchmarks/sregym/protocol/benchmark.py", "benchmarks/sregym/protocol/conductor.py",
	"benchmarks/sregym/protocol/mcp_client.py", "benchmarks/sregym/protocol/receipt.py",
	"benchmarks/sregym/protocol/schema.py", "benchmarks/sregym/protocol/submission_guard.py",
	"benchmarks/sregym/fastloop/__init__.py", "benchmarks/sregym/fastloop/model_recording.py",
	"benchmarks/sregym/fastloop/file_edits.py", "benchmarks/sregym/fastloop/recording_worker.py",
}

// Apply gives every container the same read-only source and operator binary.
func Apply(pod *corev1.PodSpec) {
	directory, file := corev1.HostPathDirectory, corev1.HostPathFile
	for _, name := range []string{"sdo", "libs", "examples"} {
		addVolume(pod, "dev-"+name, name, &directory)
	}
	addVolume(pod, "dev-operator", ".fastloop/sdo-operator", &file)
	if os.Getenv("SDO_DEV_MODULE_DOWNLOADS") == "1" {
		addVolume(pod, "dev-module-downloads", ".fastloop/go-mod-download", &directory)
	}
	volume := claimVolumeName(pod)
	for index := range pod.InitContainers {
		mount(&pod.InitContainers[index], volume)
	}
	for index := range pod.Containers {
		mount(&pod.Containers[index], volume)
	}
}

func mount(container *corev1.Container, volume string) {
	if os.Getenv("SDO_DEV_MODULE_DOWNLOADS") == "1" {
		addMount(container, volume, "dev-module-downloads", "/go/pkg/mod/cache/download", ".fastloop/go-mod-download")
		container.Env = append(container.Env, corev1.EnvVar{Name: "SDO_DEV_MODULE_DOWNLOADS", Value: "1"})
	}
	for _, name := range []string{"sdo", "libs", "examples"} {
		addMount(container, volume, "dev-"+name, "/opt/sdo/"+name, name)
	}
	addMount(container, volume, "dev-operator", "/usr/local/bin/sdo-operator", ".fastloop/sdo-operator")
}

// addVolume makes the path rel of the projected tree available to the pod as volume name: a hostPath of the node
// directory, or (a claim-backed lane) the claim's single volume, which a mount then narrows to rel by subPath.
func addVolume(pod *corev1.PodSpec, name, rel string, kind *corev1.HostPathType) {
	if Claim() != "" {
		claimVolumeName(pod)
		return
	}
	pod.Volumes = append(pod.Volumes, corev1.Volume{
		Name:         name,
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: NodePath() + "/" + rel, Type: kind}},
	})
}

// claimVolumeName is the pod's volume on the dev-tree claim, added when the pod has none. A pod that already mounts the
// claim (the controller's evidence sink) is reused: a second volume on the same node-local claim in one pod does not
// mount (the kubelet waits on the first forever).
func claimVolumeName(pod *corev1.PodSpec) string {
	for _, v := range pod.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == Claim() {
			return v.Name
		}
	}
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: claimVolume, VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: Claim()},
	}})
	return claimVolume
}

// addMount mounts volume name (made by addVolume for rel) read-only at path.
func addMount(container *corev1.Container, claimVolume, name, path, rel string) {
	if Claim() != "" {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name: claimVolume, MountPath: path, ReadOnly: true, SubPath: NodePath() + "/" + rel,
		})
		return
	}
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: name, MountPath: path, ReadOnly: true})
}

// ApplyWorker adds only the benchmark worker entry points to a development
// agent Job. Controllers remain free of benchmark code.
func ApplyWorker(pod *corev1.PodSpec) {
	Apply(pod)
	directory, file := corev1.HostPathDirectory, corev1.HostPathFile
	for index, path := range benchmarkFiles {
		addVolume(pod, fmt.Sprintf("dev-bench-%d", index), path, &file)
	}
	for index, name := range scriptDirectories {
		addVolume(pod, fmt.Sprintf("dev-script-%d", index), scriptRoot+name, &directory)
	}

	volume := claimVolumeName(pod)
	for index := range pod.Containers {
		mountWorker(&pod.Containers[index], volume)
	}
	for index := range pod.InitContainers {
		mountWorker(&pod.InitContainers[index], volume)
	}
}

const scriptRoot = "benchmarks/sregym/fastloop/"

var scriptDirectories = []string{"scripted_program", "scripted_networkpolicy_program"}

func mountWorker(container *corev1.Container, volume string) {
	for index, path := range benchmarkFiles {
		addMount(container, volume, fmt.Sprintf("dev-bench-%d", index), "/opt/sdo/"+path, path)
	}
	for index, name := range scriptDirectories {
		addMount(container, volume, fmt.Sprintf("dev-script-%d", index), "/opt/sdo/"+scriptRoot+name, scriptRoot+name)
	}
}
