//go:build integration

package builder

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateAndBuildSeedsGoCacheBeforeProgramCompilation(t *testing.T) {
	seed := t.TempDir()
	cache := filepath.Join(t.TempDir(), "go-cache")
	if err := os.WriteFile(filepath.Join(seed, "compiled-sdk"), []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	goCommand := filepath.Join(bin, "go")
	if err := os.WriteFile(goCommand, []byte("#!/bin/sh\ntest -f \"$GOCACHE/compiled-sdk\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SDO_GO_CACHE_SEED", seed)
	t.Setenv("GOCACHE", cache)
	workspace := &Workspace{Directory: t.TempDir()}

	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("program build did not use the seeded SDK cache: %v", err)
	}
}

func TestValidateAndBuildUsesColdCacheWhenImageHasNoSeed(t *testing.T) {
	cache := filepath.Join(t.TempDir(), "go-cache")
	bin := t.TempDir()
	goCommand := filepath.Join(bin, "go")
	if err := os.WriteFile(goCommand, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SDO_GO_CACHE_SEED", filepath.Join(t.TempDir(), "absent"))
	t.Setenv("GOCACHE", cache)

	if _, err := (&Workspace{Directory: t.TempDir()}).ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("a pre-cache image cannot run a cold program build: %v", err)
	}
}

func TestValidateAndBuildIgnoresCallerGoWorkspace(t *testing.T) {
	bin := t.TempDir()
	goCommand := filepath.Join(bin, "go")
	// The generated workspace is its own module; a caller's go.work must not select it.
	if err := os.WriteFile(goCommand, []byte("#!/bin/sh\ntest \"$GOWORK\" = off\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GOWORK", filepath.Join(t.TempDir(), "go.work"))
	workspace := &Workspace{Directory: t.TempDir()}

	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("generated workspace inherited the caller's go.work: %v", err)
	}
}

func TestReadThroughGoCacheDoesNotCopySeedAtStartup(t *testing.T) {
	seed, cache := t.TempDir(), filepath.Join(t.TempDir(), "private-cache")
	if err := os.WriteFile(filepath.Join(seed, "compiled-sdk"), []byte("immutable"), 0400); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	// Startup can invoke Go, but any hidden cache copy fails this test.
	if err := os.WriteFile(filepath.Join(bin, "cp"), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte("#!/bin/sh\ntest ! -e \"$GOCACHE/compiled-sdk\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("GOCACHEPROG", "/usr/local/bin/sdo-go-cache")
	t.Setenv("SDO_GO_CACHE_SEED", seed)
	t.Setenv("GOCACHE", cache)
	if _, err := (&Workspace{Directory: t.TempDir()}).ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("startup copied immutable seed: %v", err)
	}
	if _, err := os.Stat(cache); !os.IsNotExist(err) {
		t.Fatal("startup materialized the cache before Go requested an entry")
	}
}
