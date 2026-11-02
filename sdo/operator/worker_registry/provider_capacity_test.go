//go:build integration

package worker_registry

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestAccountReferencesReachWorkersWithoutSubPathOrTokenEnvironment(t *testing.T) {
	env := map[string]string{
		"SDO_OPUS_POOL_MODE": "per_account", "SDO_OPUS_POOL_HOST_ID": "synthetic-host",
		"SDO_OPUS_POOL_HOST_PATH":    "/opt/sdo-parallel/opus-pool",
		"SDO_CLAUDE_ACCOUNTS_SECRET": "claude-accounts", "SDO_CLAUDE_ACCOUNT_PRIMARY": "a02",
		"SDO_CLAUDE_ACCOUNT_MAX_SESSIONS": "2",
	}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	if err := ApplyProviderCapacity(pod, env); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, volume := range pod.Volumes {
		if volume.Name == "claude-accounts" {
			found = volume.Secret != nil && volume.Secret.SecretName == "claude-accounts"
		}
	}
	if !found {
		t.Fatal("account Secret not projected")
	}
	for _, mount := range pod.Containers[0].VolumeMounts {
		if mount.Name == "claude-accounts" && (mount.SubPath != "" || !mount.ReadOnly) {
			t.Fatal("account updates cannot be observed safely")
		}
	}
	values := map[string]string{}
	for _, variable := range pod.Containers[0].Env {
		values[variable.Name] = variable.Value
	}
	if values["SDO_CLAUDE_ACCOUNTS_DIR"] != "/sdo/claude-accounts" || values["SDO_CLAUDE_ACCOUNT_PRIMARY"] != "a02" {
		t.Fatal("account references lost")
	}
	for _, name := range []string{"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"} {
		if _, ok := values[name]; ok {
			t.Fatal("credential value rendered into Pod spec")
		}
	}
}

func TestHostProviderCapacityReachesColdAndStandbyJobs(t *testing.T) {
	for _, standby := range []bool{false, true} {
		t.Run(map[bool]string{false: "cold", true: "standby"}[standby], func(t *testing.T) {
			_, executor, task := standbyFixture(t)
			executor.Environment = map[string]string{
				"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_ID": "chelan4",
				"SDO_OPUS_POOL_HOST_PATH": "/opt/sdo-parallel/opus-pool", "SDO_OPUS_MAX_SESSIONS": "3",
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if standby {
				pool := NewStandbyPool(executor, "responder", "app")
				if err := pool.Ensure(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				created := make(chan struct{}, 1)
				executor.OnPhase = notifyTestJobCreated(created)
				finished := make(chan error, 1)
				t.Cleanup(func() { cancel(); <-finished })
				go func() { _, err := executor.Execute(ctx, task); finished <- err }()
				select {
				case <-created:
				case err := <-finished:
					finished <- err
					t.Fatalf("worker stopped before Job creation: %v", err)
				}
			}
			jobs, err := executor.Client.BatchV1().Jobs(executor.Namespace).List(ctx, metav1.ListOptions{})
			if err != nil || len(jobs.Items) != 1 {
				t.Fatalf("worker Job missing: %v %+v", err, jobs)
			}
			pod := jobs.Items[0].Spec.Template.Spec
			found := false
			for _, volume := range pod.Volumes {
				if volume.Name == "opus-pool" {
					found = volume.HostPath != nil && volume.HostPath.Path == "/opt/sdo-parallel/opus-pool" && *volume.HostPath.Type == corev1.HostPathDirectory
				}
			}
			if !found {
				t.Fatal("worker did not mount existing host pool")
			}
			env := map[string]string{}
			for _, value := range pod.Containers[0].Env {
				env[value.Name] = value.Value
			}
			if env["SDO_OPUS_POOL_DIR"] != "/sdo/opus-pool" || env["SDO_OPUS_MAX_SESSIONS"] != "3" || env["SDO_OPUS_POOL_HOST_ID"] != "chelan4" || env["SDO_OPUS_POOL_MODE"] != "per_host" {
				t.Fatalf("host allocation lost: %+v", env)
			}
		})
	}
}

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
	for _, env := range []map[string]string{
		{"SDO_OPUS_POOL_NFS_SERVER": "server"}, {"SDO_OPUS_POOL_NFS_PATH": "/pool"}, {"SDO_OPUS_MAX_SESSIONS": "0"},
		{"SDO_OPUS_POOL_MODE": "private"},
		{"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_ID": "chelan4"},
		{"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_PATH": "/pool"},
		{"SDO_OPUS_POOL_HOST_PATH": "/pool"},
		{"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_ID": "chelan4", "SDO_OPUS_POOL_HOST_PATH": "/pool", "SDO_OPUS_POOL_NFS_SERVER": "server", "SDO_OPUS_POOL_NFS_PATH": "/pool"},
		{"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_ID": "chelan4", "SDO_OPUS_POOL_HOST_PATH": "/pool/../other"},
		{"SDO_OPUS_POOL_MODE": "per_host", "SDO_OPUS_POOL_HOST_ID": "chelan4", "SDO_OPUS_POOL_HOST_PATH": "/"},
	} {
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
