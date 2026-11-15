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
