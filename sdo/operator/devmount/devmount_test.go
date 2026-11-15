package devmount

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestApplyUsesTheConfiguredNodePath(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT_PATH", "/bench-evidence/.dev")
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}
	Apply(pod)
	for _, v := range pod.Volumes {
		if v.HostPath != nil && v.HostPath.Path[:len("/bench-evidence/.dev")] != "/bench-evidence/.dev" {
			t.Fatalf("volume %s at %s, want under /bench-evidence/.dev", v.Name, v.HostPath.Path)
		}
	}
	if len(pod.Volumes) == 0 {
		t.Fatal("no volumes")
	}
}

func TestEnvironmentCarriesThePathToTheController(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT_PATH", "/x")
	env := Environment()
	if len(env) != 2 || env[0].Name != "SDO_DEV_MOUNT" || env[1].Name != "SDO_DEV_MOUNT_PATH" || env[1].Value != "/x" {
		t.Fatalf("%v", env)
	}
	t.Setenv("SDO_DEV_MOUNT_PATH", "")
	if env := Environment(); len(env) != 1 {
		t.Fatalf("%v", env)
	}
}

func TestClaimBackedMountsUseOneVolumeAndSubPaths(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT_PATH", ".dev")
	t.Setenv("SDO_DEV_MOUNT_CLAIM", "sdo-evidence")
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}}
	ApplyWorker(pod)
	if len(pod.Volumes) != 1 || pod.Volumes[0].PersistentVolumeClaim == nil || pod.Volumes[0].PersistentVolumeClaim.ClaimName != "sdo-evidence" {
		t.Fatalf("want one claim volume, got %v", pod.Volumes)
	}
	seen := map[string]string{}
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.Name != pod.Volumes[0].Name || !m.ReadOnly || m.SubPath == "" {
			t.Fatalf("mount %v is not a read-only subPath of the claim volume", m)
		}
		seen[m.MountPath] = m.SubPath
	}
	if seen["/opt/sdo/sdo"] != ".dev/sdo" || seen["/usr/local/bin/sdo-operator"] != ".dev/.fastloop/sdo-operator" {
		t.Fatalf("%v", seen)
	}
	if seen["/opt/sdo/benchmarks/sregym/fastloop/scripted_program"] != ".dev/benchmarks/sregym/fastloop/scripted_program" {
		t.Fatalf("%v", seen)
	}
}

func TestEnvironmentCarriesTheClaimToTheController(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT_PATH", ".dev")
	t.Setenv("SDO_DEV_MOUNT_CLAIM", "c")
	found := false
	for _, e := range Environment() {
		found = found || (e.Name == "SDO_DEV_MOUNT_CLAIM" && e.Value == "c")
	}
	if !found {
		t.Fatal("claim not passed on to dev-mounted workers")
	}
}

func TestClaimBackedMountsShareThePodsExistingVolumeOnTheClaim(t *testing.T) {
	// A second volume on the same node-local claim in one pod never mounts: reuse the volume the pod already has.
	t.Setenv("SDO_DEV_MOUNT_PATH", ".dev")
	t.Setenv("SDO_DEV_MOUNT_CLAIM", "sdo-evidence")
	pod := &corev1.PodSpec{
		Volumes: []corev1.Volume{{Name: "evidence-sink", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "sdo-evidence"},
		}}},
		Containers: []corev1.Container{{Name: "c"}},
	}
	Apply(pod)
	if len(pod.Volumes) != 1 || pod.Volumes[0].Name != "evidence-sink" {
		t.Fatalf("want the pod's own claim volume only, got %v", pod.Volumes)
	}
	for _, m := range pod.Containers[0].VolumeMounts {
		if m.Name != "evidence-sink" || m.SubPath == "" {
			t.Fatalf("mount %v does not use the existing claim volume by subPath", m)
		}
	}
}
