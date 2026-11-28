package toolchain

import (
	"os"
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

const image = "registry.invalid/controller@sha256:00"

// sharedVolume lays out a repository volume as the controller leaves it: the published toolchain seed (with or without
// the cache helper) and, optionally, the controller's build cache.
func sharedVolume(t *testing.T, helper, controllerCache bool) string {
	t.Helper()
	mount := t.TempDir()
	if err := os.MkdirAll(filepath.Join(mount, SharedSubPath(image), "cache", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if helper {
		if err := os.WriteFile(filepath.Join(mount, SharedSubPath(image), cacheProgramInSeed), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if controllerCache {
		if err := os.MkdirAll(filepath.Join(mount, ControllerBuildCacheSubPath), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return mount
}

// A worker on the shared toolchain reads the controller's build cache in place, read only, before the image seed, so
// its first memory check reuses the packages, vet facts and test results the controller already produced for the
// accepted memory. Nothing is copied into the worker's private cache first.
func TestApplySharedReadsTheControllersBuildCacheBeforeTheImageSeed(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Env: []corev1.EnvVar{{Name: "SDO_GO_CACHE_SEED", Value: "/stale"}}}}}
	ApplyShared(pod, image, "apps/a", sharedVolume(t, true, true))
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
	if "/go/"+strings.TrimPrefix(cacheProgramInSeed, "cache/") != cacheProgram {
		t.Fatalf("the helper %s must be the seed's %s, inside the toolchain's /go mount", cacheProgram, cacheProgramInSeed)
	}
}

// Without a controller cache (a controller started with another GOCACHE) the worker reads the image seed in place and
// mounts nothing more.
func TestApplySharedWithoutAControllerCacheReadsTheImageSeed(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
	ApplyShared(pod, image, "", sharedVolume(t, true, false))
	if _, ok := mountAt(pod.Containers[0], controllerBuildCacheMount); ok {
		t.Fatal("mounted a controller cache that does not exist")
	}
	if seeds, _ := env(pod.Containers[0], "SDO_GO_CACHE_SEED"); seeds != imageBuildCache {
		t.Fatalf("SDO_GO_CACHE_SEED = %q, want the image seed", seeds)
	}
}

// A seed published from an image without the helper must not point Go at a program that is not there: every Go command
// in the pod, the agent's own included, would fail. The pod keeps the image's private cache and its seed copy.
func TestApplySharedWithoutTheHelperLeavesGoAsTheImageSetsIt(t *testing.T) {
	for name, mount := range map[string]string{"older image": sharedVolume(t, false, true), "unknown volume": ""} {
		pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}
		ApplyShared(pod, image, "", mount)
		if _, n := env(pod.Containers[0], "GOCACHEPROG"); n != 0 {
			t.Fatalf("%s: GOCACHEPROG set without a helper", name)
		}
		if _, ok := mountAt(pod.Containers[0], controllerBuildCacheMount); ok {
			t.Fatalf("%s: controller cache mounted without a helper to read it", name)
		}
	}
}
