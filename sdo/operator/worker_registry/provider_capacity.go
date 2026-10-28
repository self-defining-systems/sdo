package worker_registry

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

var providerCapacityNames = []string{
	"SDO_OPUS_POOL_DIR", "SDO_OPUS_MAX_SESSIONS", "SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH",
	"SDO_OPUS_POOL_MODE", "SDO_OPUS_POOL_HOST_ID", "SDO_OPUS_POOL_HOST_PATH",
}

// ProviderCapacityEnvironment carries only pool settings, never credentials.
func ProviderCapacityEnvironment() map[string]string {
	env := map[string]string{}
	for _, name := range providerCapacityNames {
		if value := os.Getenv(name); value != "" {
			env[name] = value
		}
	}
	return env
}

// ApplyProviderCapacity wires the configured pool into controller, operate,
// cold workers and warm workers, so every role has one admission path.
func ApplyProviderCapacity(pod *corev1.PodSpec, env map[string]string) error {
	server, root := env["SDO_OPUS_POOL_NFS_SERVER"], env["SDO_OPUS_POOL_NFS_PATH"]
	mode, hostPath := env["SDO_OPUS_POOL_MODE"], env["SDO_OPUS_POOL_HOST_PATH"]
	if mode == "" {
		mode = "shared"
	}
	if mode != "shared" && mode != "per_host" {
		return fmt.Errorf("Opus pool mode must be shared or per_host")
	}
	if mode == "per_host" {
		if strings.TrimSpace(env["SDO_OPUS_POOL_HOST_ID"]) == "" || hostPath == "" {
			return fmt.Errorf("per_host Opus pool requires host ID and existing node host path")
		}
		if server != "" || root != "" {
			return fmt.Errorf("per_host Opus pool cannot use NFS settings")
		}
	} else if hostPath != "" {
		return fmt.Errorf("Opus pool host path requires per_host mode")
	}
	if hostPath != "" && (!path.IsAbs(hostPath) || path.Clean(hostPath) != hostPath || hostPath == "/" || strings.Contains(hostPath, "\\")) {
		return fmt.Errorf("Opus pool host path must be a clean absolute directory path")
	}
	if (server == "") != (root == "") {
		return fmt.Errorf("Opus pool requires both NFS server and path")
	}
	if root != "" && (!path.IsAbs(root) || path.Clean(root) != root || strings.Contains(root, "\\")) {
		return fmt.Errorf("Opus pool NFS path must be a clean absolute path")
	}
	if value := env["SDO_OPUS_MAX_SESSIONS"]; value != "" {
		capacity, err := strconv.Atoi(value)
		if err != nil || capacity <= 0 {
			return fmt.Errorf("Opus pool capacity must be a positive integer")
		}
	}
	if server != "" {
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "opus-pool", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: server, Path: root}}})
	}
	if mode == "per_host" {
		directory := corev1.HostPathDirectory
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "opus-pool", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostPath, Type: &directory}}})
	}
	for index := range pod.Containers {
		container := &pod.Containers[index]
		// Replace only these owned names; e.Environment may already have supplied
		// them when constructing a generic worker.
		for _, name := range providerCapacityNames {
			value := env[name]
			if name == "SDO_OPUS_POOL_DIR" && (server != "" || mode == "per_host") {
				value = "/sdo/opus-pool"
			}
			if name == "SDO_OPUS_MAX_SESSIONS" && value == "" && mode == "per_host" {
				value = "4"
			}
			if value == "" {
				continue
			}
			kept := container.Env[:0]
			for _, variable := range container.Env {
				if variable.Name != name {
					kept = append(kept, variable)
				}
			}
			container.Env = append(kept, corev1.EnvVar{Name: name, Value: value})
		}
		if server != "" || mode == "per_host" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "opus-pool", MountPath: "/sdo/opus-pool"})
		}
	}
	return nil
}
