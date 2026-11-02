package install

import (
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/worker_registry"
	"testing"
)

func TestAccountProviderCapacitySurvivesInstallerControllerAndWorkerBoundary(t *testing.T) {
	t.Setenv("SDO_OPUS_POOL_MODE", "per_account")
	t.Setenv("SDO_OPUS_POOL_HOST_ID", "chelan4")
	t.Setenv("SDO_OPUS_POOL_HOST_PATH", "/opt/sdo-parallel/account-pool")
	t.Setenv("SDO_CLAUDE_ACCOUNTS_SECRET", "claude-accounts")
	t.Setenv("SDO_CLAUDE_ACCOUNT_PRIMARY", "a03")
	t.Setenv("SDO_CLAUDE_ACCOUNT_MAX_SESSIONS", "2")
	plan, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	operate, err := OperateJob(testConfig(), "healthy", "operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, parent := range []*corev1.PodSpec{&plan.Job.Spec.Template.Spec, &operate.Spec.Template.Spec} {
		inherited := map[string]string{}
		for _, value := range parent.Containers[0].Env {
			inherited[value.Name] = value.Value
		}
		worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "cold"}, {Name: "warm"}}}
		if err := worker_registry.ApplyProviderCapacity(worker, inherited); err != nil {
			t.Fatal(err)
		}
		for _, pod := range []*corev1.PodSpec{parent, worker} {
			found := false
			for _, volume := range pod.Volumes {
				if volume.Name == "claude-accounts" {
					found = volume.Secret != nil && volume.Secret.SecretName == "claude-accounts"
				}
			}
			if !found {
				t.Fatal("projected account reference lost")
			}
			for _, container := range pod.Containers {
				env := map[string]string{}
				for _, value := range container.Env {
					env[value.Name] = value.Value
				}
				if env["SDO_CLAUDE_ACCOUNT_PRIMARY"] != "a03" || env["SDO_CLAUDE_ACCOUNTS_DIR"] != "/sdo/claude-accounts" || env["SDO_OPUS_POOL_MODE"] != "per_account" {
					t.Fatal("lane account assignment lost")
				}
			}
		}
	}
}

func TestProviderCapacitySurvivesInstallerControllerAndWorkerBoundary(t *testing.T) {
	t.Setenv("SDO_OPUS_POOL_NFS_SERVER", "nfs.example")
	t.Setenv("SDO_OPUS_POOL_NFS_PATH", "/shared/opus")
	t.Setenv("SDO_OPUS_MAX_SESSIONS", "8")
	plan, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	operate, err := OperateJob(testConfig(), "healthy", "operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range []*corev1.PodSpec{&plan.Job.Spec.Template.Spec, &operate.Spec.Template.Spec} {
		inherited := map[string]string{}
		for _, value := range pod.Containers[0].Env {
			inherited[value.Name] = value.Value
		}
		worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
		if err := worker_registry.ApplyProviderCapacity(worker, inherited); err != nil {
			t.Fatal(err)
		}
		if len(worker.Volumes) != 1 || worker.Volumes[0].NFS == nil || worker.Volumes[0].NFS.Server != "nfs.example" || worker.Volumes[0].NFS.Path != "/shared/opus" {
			t.Fatalf("shared capacity lost: %+v", worker.Volumes)
		}
	}
}

func TestHostProviderCapacitySurvivesInstallerControllerAndWorkerBoundary(t *testing.T) {
	t.Setenv("SDO_OPUS_POOL_MODE", "per_host")
	t.Setenv("SDO_OPUS_POOL_HOST_ID", "chelan5")
	t.Setenv("SDO_OPUS_POOL_HOST_PATH", "/opt/sdo-parallel/opus-pool")
	plan, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	operate, err := OperateJob(testConfig(), "healthy", "operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range []*corev1.PodSpec{&plan.Job.Spec.Template.Spec, &operate.Spec.Template.Spec} {
		inherited := map[string]string{}
		for _, value := range pod.Containers[0].Env {
			inherited[value.Name] = value.Value
		}
		worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "cold"}, {Name: "warm"}}}
		if err := worker_registry.ApplyProviderCapacity(worker, inherited); err != nil {
			t.Fatal(err)
		}
		for _, spec := range []*corev1.PodSpec{pod, worker} {
			if len(spec.Volumes) == 0 {
				t.Fatal("host pool missing")
			}
			found := false
			for _, volume := range spec.Volumes {
				if volume.Name == "opus-pool" {
					found = volume.HostPath != nil && volume.HostPath.Path == "/opt/sdo-parallel/opus-pool" && *volume.HostPath.Type == corev1.HostPathDirectory
				}
			}
			if !found {
				t.Fatalf("host pool lost: %+v", spec.Volumes)
			}
			for _, container := range spec.Containers {
				env := map[string]string{}
				for _, value := range container.Env {
					env[value.Name] = value.Value
				}
				if env["SDO_OPUS_POOL_DIR"] != "/sdo/opus-pool" || env["SDO_OPUS_MAX_SESSIONS"] != "4" || env["SDO_OPUS_POOL_HOST_ID"] != "chelan5" || env["SDO_OPUS_POOL_MODE"] != "per_host" {
					t.Fatalf("host allocation lost: %+v", env)
				}
			}
		}
	}
}
