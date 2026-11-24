package install

import (
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/builder"
	"testing"
	"time"
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

// A stopping controller needs StopGrace for its child's shutdown (the leader
// Lease release) before the kubelet kills the pod.
func TestControllerPodGracePeriodOutlastsTheChildStop(t *testing.T) {
	job := controllerJob(Config{Namespace: "app", Application: "synthetic", ControllerImage: "synthetic-controller", ResponderImage: "synthetic-worker"})
	grace := job.Spec.Template.Spec.TerminationGracePeriodSeconds
	if grace == nil {
		t.Fatal("the controller pod must set terminationGracePeriodSeconds, not rely on the default")
	}
	if time.Duration(*grace)*time.Second <= builder.ControllerStopGrace {
		t.Fatalf("terminationGracePeriodSeconds = %d, not above the child's stop grace %v", *grace, builder.ControllerStopGrace)
	}
}

func TestControllerPrewarmsDedicatedScriptImage(t *testing.T) {
	job := controllerJob(Config{Namespace: "app", Application: "synthetic", ControllerImage: "controller", ScriptImage: "script-tools"})
	for _, init := range job.Spec.Template.Spec.InitContainers {
		if init.Name == "prewarm-script" && init.Image == "script-tools" && init.ImagePullPolicy == corev1.PullIfNotPresent {
			return
		}
	}
	t.Fatal("controller does not prewarm its dedicated script image")
}
