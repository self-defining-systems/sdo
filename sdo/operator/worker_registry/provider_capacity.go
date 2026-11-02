package worker_registry

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/worker_registry/internal/workerio"
)

var providerCapacityNames = []string{
	"SDO_OPUS_POOL_DIR", "SDO_OPUS_MAX_SESSIONS", "SDO_OPUS_POOL_NFS_SERVER", "SDO_OPUS_POOL_NFS_PATH",
	"SDO_OPUS_POOL_MODE", "SDO_OPUS_POOL_HOST_ID", "SDO_OPUS_POOL_HOST_PATH",
	"SDO_CLAUDE_ACCOUNTS_DIR", "SDO_CLAUDE_ACCOUNTS_SECRET", "SDO_CLAUDE_ACCOUNT_PRIMARY",
	"SDO_CLAUDE_ACCOUNT_MAX_SESSIONS", "SDO_CLAUDE_ACCOUNT_COOLDOWN_SECONDS", "SDO_CLAUDE_ACCOUNT_BACKOFF_SECONDS",
}

// ProviderCapacityEnvironment carries only pool settings, never credentials.
func ProviderCapacityEnvironment() map[string]string {
	env := map[string]string{}
	for _, name := range providerCapacityNames {
		if value := workerio.Getenv(name); value != "" {
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
	if mode != "shared" && mode != "per_host" && mode != "per_account" {
		return fmt.Errorf("Opus pool mode must be shared, per_host or per_account")
	}
	if mode == "per_host" || mode == "per_account" {
		if strings.TrimSpace(env["SDO_OPUS_POOL_HOST_ID"]) == "" || hostPath == "" {
			return fmt.Errorf("per_host Opus pool requires host ID and existing node host path")
		}
		if server != "" || root != "" {
			return fmt.Errorf("per_host Opus pool cannot use NFS settings")
		}
	} else if hostPath != "" {
		return fmt.Errorf("Opus pool host path requires per_host mode")
	}
	if mode == "per_account" {
		if env["SDO_CLAUDE_ACCOUNTS_SECRET"] == "" || env["SDO_CLAUDE_ACCOUNT_PRIMARY"] == "" {
			return fmt.Errorf("per_account capacity requires account Secret reference and lane primary alias")
		}
		if value := env["SDO_CLAUDE_ACCOUNT_MAX_SESSIONS"]; value != "" {
			capacity, err := strconv.Atoi(value)
			if err != nil || capacity <= 0 {
				return fmt.Errorf("per-account concurrency must be a positive integer")
			}
		}
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "claude-accounts", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: env["SDO_CLAUDE_ACCOUNTS_SECRET"]}}})
	} else if env["SDO_CLAUDE_ACCOUNTS_SECRET"] != "" || env["SDO_CLAUDE_ACCOUNTS_DIR"] != "" {
		return fmt.Errorf("Claude account references require per_account capacity mode")
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
	if mode == "per_host" || mode == "per_account" {
		directory := corev1.HostPathDirectory
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "opus-pool", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: hostPath, Type: &directory}}})
	}
	for index := range pod.Containers {
		container := &pod.Containers[index]
		// Replace only these owned names; e.Environment may already have supplied
		// them when constructing a generic worker.
		for _, name := range providerCapacityNames {
			value := env[name]
			if name == "SDO_CLAUDE_ACCOUNTS_DIR" && mode == "per_account" {
				value = "/sdo/claude-accounts"
			}
			if name == "SDO_OPUS_POOL_DIR" && (server != "" || mode == "per_host" || mode == "per_account") {
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
		if mode == "per_account" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "claude-accounts", MountPath: "/sdo/claude-accounts", ReadOnly: true})
		}
		if server != "" || mode == "per_host" || mode == "per_account" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "opus-pool", MountPath: "/sdo/opus-pool"})
		}
	}
	return nil
}
