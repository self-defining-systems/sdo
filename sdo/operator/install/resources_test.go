package install

import (
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestControllerCPUAllowsControllerAndMemoryHostBurst(t *testing.T) {
	job := controllerJob(Config{Namespace: "app", Application: "synthetic", ControllerImage: "synthetic-controller", ResponderImage: "synthetic-worker"})
	resources := job.Spec.Template.Spec.Containers[0].Resources
	// The standard image configures two Go processors per runtime. Both
	// processes share this Pod's cgroup; a two-CPU ceiling throttles their burst.
	if limit := resources.Limits[corev1.ResourceCPU]; limit.MilliValue() < 4000 {
		t.Fatalf("controller and memory host need four CPUs of burst headroom, got %s", limit.String())
	}
}
