package install

import (
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/worker_registry"
	"testing"
)

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
