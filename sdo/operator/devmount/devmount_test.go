package devmount

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestApplyMountsSourcePackagesAndHostBinaryReadOnly(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	Apply(pod)
	if len(pod.Volumes) != 4 || len(pod.Containers[0].VolumeMounts) != 4 {
		t.Fatalf("dev volumes=%v mounts=%v", pod.Volumes, pod.Containers[0].VolumeMounts)
	}
	paths := map[string]string{
		"/opt/sdo/sdo":                "/opt/sdo-dev/sdo",
		"/opt/sdo/libs":               "/opt/sdo-dev/libs",
		"/opt/sdo/examples":           "/opt/sdo-dev/examples",
		"/usr/local/bin/sdo-operator": "/opt/sdo-dev/.fastloop/sdo-operator",
	}
	for index, mount := range pod.Containers[0].VolumeMounts[:4] {
		if !mount.ReadOnly {
			t.Fatalf("writable dev mount: %+v", mount)
		}
		if paths[mount.MountPath] != pod.Volumes[index].HostPath.Path {
			t.Fatalf("dev mount=%+v volume=%+v", mount, pod.Volumes[index])
		}
	}
}

func TestApplyOptInModuleDownloadsForMountedSource(t *testing.T) {
	t.Setenv("SDO_DEV_MODULE_DOWNLOADS", "1")
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}, InitContainers: []corev1.Container{{Name: "init"}}}
	Apply(pod)
	for _, container := range append(pod.Containers, pod.InitContainers...) {
		found := false
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/go/pkg/mod/cache/download" && mount.ReadOnly {
				found = true
			}
		}
		if !found {
			t.Fatalf("module metadata unavailable to %s", container.Name)
		}
		propagated := false
		for _, env := range container.Env {
			if env.Name == "SDO_DEV_MODULE_DOWNLOADS" && env.Value == "1" {
				propagated = true
			}
		}
		if !propagated {
			t.Fatalf("nested worker mount opt-in missing for %s", container.Name)
		}
	}
	found := false
	for _, volume := range pod.Volumes {
		if volume.HostPath != nil && volume.HostPath.Path == NodePath+"/.fastloop/go-mod-download" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing node module metadata volume")
	}
}

func TestApplyMountsLiveBenchmarkModulesWithoutAnswerKeys(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "live-worker"}}, InitContainers: []corev1.Container{{Name: "init"}}}
	ApplyWorker(pod)
	for _, container := range append(pod.Containers, pod.InitContainers...) {
		count := 0
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == "/opt/sdo/benchmarks" {
				t.Fatal("answer key tree exposed")
			}
			if mount.MountPath == "/opt/sdo/benchmarks/sregym/fastloop/model_recording.py" {
				count++
			}
			if mount.MountPath == "/opt/sdo/benchmarks/sregym/adapter/scripted_worker.py" {
				count++
			}
		}
		if count != 2 {
			t.Fatalf("benchmark code not mounted: %+v", container.VolumeMounts)
		}
	}
}

func TestApplyWorkerMountsToyFixtureWithoutFaultCatalog(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "scripted"}}}
	ApplyWorker(pod)
	for _, target := range []string{"/opt/sdo/apps/toy/seed", "/opt/sdo/apps/toy/kubernetes"} {
		found := false
		for _, mount := range pod.Containers[0].VolumeMounts {
			if mount.MountPath == "/opt/sdo/apps" || mount.MountPath == "/opt/sdo/apps/toy" {
				t.Fatal("toy fault catalog exposed")
			}
			if mount.MountPath == target && mount.ReadOnly {
				found = true
			}
		}
		if !found {
			t.Fatalf("scripted fixture missing read-only mount %s", target)
		}
	}
}
