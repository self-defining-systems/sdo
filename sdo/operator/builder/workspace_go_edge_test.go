package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/internal/testutil"
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

func TestBuildWorkspaceRunsOptInHealthyBaseline(t *testing.T) {
	testutil.FakeGo(t, testutil.GoRule{Verb: "test", Covers: "generated", Marker: "generated/healthy_baseline_test.go", Output: "synthetic healthy baseline firing"})
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app", ".sdo", "diagnostics")
	diagnostics := filepath.Join(app, ".sdo", "diagnostics")
	if err := copyTree(context.Background(), source, diagnostics); err != nil {
		t.Fatal(err)
	}
	baseline := filepath.Join(app, "baseline")
	if err := os.MkdirAll(baseline, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(baseline, "healthy.json"), capturefixture.FromView(t, `{"namespace":"shop","services":[],"pods":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: app, DiagnosticsRoot: diagnostics, OperatorRoot: operatorRoot, HealthyBaseline: baseline})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated", "healthy_baseline_test.go")); err != nil {
		t.Fatal(err)
	}
	// The generated healthy-baseline test is part of the acceptance stages: a
	// firing detector there rejects the workspace.
	if err := workspace.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "synthetic healthy baseline firing") {
		t.Fatalf("healthy baseline gate did not reject a firing detector: %v", err)
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

func TestReplayOffCompilesProgramsWithoutReadingCorpus(t *testing.T) {
	testutil.FakeGo(t)
	app := t.TempDir()
	source := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app")
	if err := copyTree(context.Background(), source, app); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(app, ".sdo/corpus/targetport-mismatch/labels.json"), []byte("malformed synthetic labels"), 0644); err != nil {
		t.Fatal(err)
	}
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo/diagnostics"), OperatorRoot: operator, Replay: "off", HealthyBaseline: filepath.Join(app, "missing-baseline")}
	workspace, err := BuildWorkspace(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if _, err := os.Stat(filepath.Join(workspace.Directory, "corpus")); !os.IsNotExist(err) {
		t.Fatalf("off staged corpus: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated/programs_test.go")); !os.IsNotExist(err) {
		t.Fatalf("off generated replay: %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated/program_declarations_test.go")); err != nil {
		t.Fatal("registration safety was removed", err)
	}
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatal("check failed", err)
	}
	notes, err := workspace.MitigationReplayResults()
	if err != nil || len(notes) != 0 {
		t.Fatalf("off ran replay: %v %v", notes, err)
	}
}
