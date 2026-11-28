package buildio

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// A pod whose image lacks the cache helper (older image, newer code) must not fail every Go command: an unusable
// GOCACHEPROG is dropped and the private cache is seeded by copy as before. A usable helper skips the copy.
func TestAnUnusableCacheProgramFallsBackToTheSeedCopy(t *testing.T) {
	seed, cache := t.TempDir(), filepath.Join(t.TempDir(), "go-cache")
	if err := os.WriteFile(filepath.Join(seed, "entry"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDO_GO_CACHE_SEED", seed)
	t.Setenv("GOCACHE", cache)
	t.Setenv("GOCACHEPROG", filepath.Join(t.TempDir(), "absent-helper"))
	if got := withoutUnusableCacheProgram([]string{"GOCACHEPROG=" + os.Getenv("GOCACHEPROG"), "A=b"}); !slices.Equal(got, []string{"A=b"}) {
		t.Fatalf("an absent helper stays in the Go environment: %q", got)
	}
	if err := SeedGoBuildCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cache, "entry")); err != nil {
		t.Fatalf("without a usable helper the seed is copied: %v", err)
	}

	helper := filepath.Join(t.TempDir(), "sdo-go-cache")
	if err := os.WriteFile(helper, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(t.TempDir(), "go-cache")
	t.Setenv("GOCACHE", fresh)
	t.Setenv("GOCACHEPROG", helper)
	if got := withoutUnusableCacheProgram([]string{"GOCACHEPROG=" + helper}); !slices.Equal(got, []string{"GOCACHEPROG=" + helper}) {
		t.Fatalf("a usable helper was dropped: %q", got)
	}
	if err := SeedGoBuildCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatalf("a usable helper reads the seed in place; nothing is copied: %v", err)
	}
}
