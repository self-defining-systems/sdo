package worker_registry

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ProviderCapacityEnvironment is a values-free allowlist. The same NFS export
// must be configured on both hosts; a private EmptyDir or per-lane PVC is never
// a substitute for the account-wide pool. Python refuses Opus without a pool.
func ProviderCapacityEnvironment() map[string]string {
	env := map[string]string{}
	for _, name := range []string{"SDO_OPUS_POOL_DIR", "SDO_OPUS_MAX_SESSIONS", "SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH"} {
		if value := os.Getenv(name); value != "" {
			env[name] = value
		}
	}
	return env
}

// ApplyProviderCapacity wires the same shared pool into controller, operate,
// cold workers and warm workers, so every role has one admission path.
func ApplyProviderCapacity(pod *corev1.PodSpec, env map[string]string) error {
	server, root := env["SDO_OPUS_POOL_NFS_SERVER"], env["SDO_OPUS_POOL_NFS_PATH"]
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
	for index := range pod.Containers {
		container := &pod.Containers[index]
		// Replace only these owned names; e.Environment may already have supplied
		// them when constructing a generic worker.
		names := []string{"SDO_OPUS_POOL_DIR", "SDO_OPUS_MAX_SESSIONS", "SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH"}
		for _, name := range names {
			value := env[name]
			if name == "SDO_OPUS_POOL_DIR" && server != "" {
				value = "/sdo/opus-pool"
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
		if server != "" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "opus-pool", MountPath: "/sdo/opus-pool"})
		}
	}
	return nil
}
