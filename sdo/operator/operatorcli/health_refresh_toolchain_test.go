package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	"sdo.dev/operator/toolchain"
	"sdo.dev/operator/worker_registry"
	"testing"
)

func TestHealthRefreshReusesCompletedImageBoundSeed(t *testing.T) {
	root := t.TempDir()
	image := "controller@sha256:trusted"
	seed := filepath.Join(root, toolchain.SharedSubPath(image))
	for _, name := range []string{"go", "cache"} {
		if err := os.MkdirAll(filepath.Join(seed, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	workers := worker_registry.KubernetesExecutor{RepositoryMountPath: root, ToolchainImage: image}
	if err := prepareHealthRefreshToolchain(context.Background(), &workers); err != nil {
		t.Fatal(err)
	}
	if !workers.SharedToolchain {
		t.Fatal("refresh still makes a private compiler copy")
	}
	if err := toolchain.Seed(context.Background(), root, image); err != nil {
		t.Fatal("controller startup did not reuse the seed", err)
	}
}

func TestHealthRefreshSeedFailurePreventsSharedDispatch(t *testing.T) {
	root := t.TempDir()
	image := "controller@sha256:trusted"
	seed := filepath.Join(root, toolchain.SharedSubPath(image))
	if err := os.MkdirAll(filepath.Dir(seed), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(seed, []byte("not a seed directory"), 0600); err != nil {
		t.Fatal(err)
	}
	workers := worker_registry.KubernetesExecutor{RepositoryMountPath: root, ToolchainImage: image}
	if err := prepareHealthRefreshToolchain(context.Background(), &workers); err == nil {
		t.Fatal("invalid seed admitted")
	}
	if workers.SharedToolchain {
		t.Fatal("failed preparation enabled shared dispatch")
	}
}
