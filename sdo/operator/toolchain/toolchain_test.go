package toolchain

import (
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func env(container corev1.Container, name string) (string, int) {
	value, count := "", 0
	for _, variable := range container.Env {
		if variable.Name == name {
			value, count = variable.Value, count+1
		}
	}
	return value, count
}

func mountAt(container corev1.Container, path string) (corev1.VolumeMount, bool) {
	for _, mount := range container.VolumeMounts {
		if mount.MountPath == path {
			return mount, true
		}
	}
	return corev1.VolumeMount{}, false
}

// A worker on the shared toolchain reads the controller's build cache in place, read only, before the image seed, so
// its first memory check reuses the packages, vet facts and test results the controller already produced for the
// accepted memory. Nothing is copied into the worker's private cache first.
func TestApplySharedReadsTheControllersBuildCacheBeforeTheImageSeed(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Env: []corev1.EnvVar{{Name: "SDO_GO_CACHE_SEED", Value: "/stale"}}}}}
	ApplyShared(pod, "registry.invalid/controller@sha256:00", "apps/a")
	container := pod.Containers[0]
	mount, ok := mountAt(container, controllerBuildCacheMount)
	if !ok || !mount.ReadOnly || mount.Name != "repository" || mount.SubPath != filepath.Join("apps/a", ControllerBuildCacheSubPath) {
		t.Fatalf("controller build cache mount: %+v (present %t)", mount, ok)
	}
	if program, n := env(container, "GOCACHEPROG"); program != cacheProgram || n != 1 {
		t.Fatalf("GOCACHEPROG = %q (%d entries)", program, n)
	}
	seeds, n := env(container, "SDO_GO_CACHE_SEED")
	if want := strings.Join([]string{controllerBuildCacheMount, imageBuildCache}, string(filepath.ListSeparator)); seeds != want || n != 1 {
		t.Fatalf("SDO_GO_CACHE_SEED = %q (%d entries), want %q once", seeds, n, want)
	}
	if !strings.HasPrefix(cacheProgram, "/go/") {
		t.Fatalf("the cache helper %s must travel inside the toolchain's /go mount", cacheProgram)
	}
}

// A worker that copies the toolchain into its own pod has no controller cache; it still reads the image seed in place.
func TestApplyReadsTheImageSeedInPlace(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	Apply(pod, "registry.invalid/controller@sha256:00")
	container := pod.Containers[0]
	if program, _ := env(container, "GOCACHEPROG"); program != cacheProgram {
		t.Fatalf("GOCACHEPROG = %q", program)
	}
	if seeds, _ := env(container, "SDO_GO_CACHE_SEED"); seeds != imageBuildCache {
		t.Fatalf("SDO_GO_CACHE_SEED = %q, want the image seed only", seeds)
	}
	if _, ok := mountAt(container, controllerBuildCacheMount); ok {
		t.Fatal("a copied toolchain has no repository volume to read the controller cache from")
	}
}
