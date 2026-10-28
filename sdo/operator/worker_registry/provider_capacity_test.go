package worker_registry

import (
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestProviderCapacitySharedNFSMountAndEnvironment(t *testing.T) {
	env := map[string]string{"SDO_OPUS_POOL_NFS_SERVER": "nfs.example", "SDO_OPUS_POOL_NFS_PATH": "/shared/opus", "SDO_OPUS_MAX_SESSIONS": "8"}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	if err := ApplyProviderCapacity(pod, env); err != nil {
		t.Fatal(err)
	}
	if len(pod.Volumes) != 1 || pod.Volumes[0].NFS == nil || pod.Volumes[0].NFS.Server != "nfs.example" || pod.Volumes[0].NFS.Path != "/shared/opus" {
		t.Fatalf("not the shared cross-host export: %+v", pod.Volumes)
	}
	found := false
	for _, v := range pod.Containers[0].Env {
		if v.Name == "SDO_OPUS_POOL_DIR" && v.Value == "/sdo/opus-pool" {
			found = true
		}
	}
	if !found || len(pod.Containers[0].VolumeMounts) != 1 {
		t.Fatal("worker cannot acquire shared pool")
	}
}

func TestProviderCapacityRefusesPartialMountConfiguration(t *testing.T) {
	for _, env := range []map[string]string{{"SDO_OPUS_POOL_NFS_SERVER": "server"}, {"SDO_OPUS_POOL_NFS_PATH": "/pool"}, {"SDO_OPUS_MAX_SESSIONS": "0"}} {
		if err := ApplyProviderCapacity(&corev1.PodSpec{}, env); err == nil {
			t.Fatalf("accepted %+v", env)
		}
	}
}

func TestAgentEnvironmentCarriesSharedCapacityForEveryRole(t *testing.T) {
	t.Setenv("SDO_OPUS_POOL_NFS_SERVER", "server")
	t.Setenv("SDO_OPUS_POOL_NFS_PATH", "/pool")
	for _, provider := range []string{"", "codex", "claude"} {
		for _, effort := range AgentEfforts {
			env, err := AgentEnvironment(provider, "opus", effort)
			if err != nil {
				t.Fatal(err)
			}
			if env["SDO_OPUS_POOL_NFS_SERVER"] != "server" || env["SDO_OPUS_POOL_NFS_PATH"] != "/pool" || env[EffortEnvironment] != effort {
				t.Fatalf("pool or effort lost at worker boundary (%q): %+v", provider, env)
			}
		}
	}
}
