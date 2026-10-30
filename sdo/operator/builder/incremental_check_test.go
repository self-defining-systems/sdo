package builder

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the actual Go cache producer and consumer across independently
// staged workspaces. A changed test must invalidate only its own package;
// neither a previous pass nor an unchanged peer can hide its failure.
func TestCheckReusesExactPackagesAcrossWorkspaces(t *testing.T) {
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	audit := filepath.Join(bin, "audit")
	wrapper := fmt.Sprintf("#!/bin/sh\nout=$(mktemp)\n%q \"$@\" >\"$out\" 2>&1\nstatus=$?\ncat \"$out\" >> %q\ncat \"$out\"\nrm \"$out\"\nexit $status\n", realGo, audit)
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(wrapper), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for attempt := 0; attempt < 4; attempt++ {
		w := acceptanceFixture(t)
		helper := filepath.Join(w.Directory, "lib", "cachehelper", "helper.go")
		if err := os.MkdirAll(filepath.Dir(helper), 0755); err != nil {
			t.Fatal(err)
		}
		value := "true"
		if attempt == 3 {
			value = "false"
		}
		if err := os.WriteFile(helper, []byte("package cachehelper\nconst Valid = "+value+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"changed", "unchanged"} {
			source := "package probe\nimport \"testing\"\nfunc TestCacheIdentity(t *testing.T) {}\n"
			if name == "changed" {
				source = "package probe\nimport (\"testing\"; \"fixture-diagnostics/lib/cachehelper\")\nfunc TestCacheIdentity(t *testing.T) { if !cachehelper.Valid { t.Fatal(\"changed dependency executed\") } }\n"
			}
			if attempt == 2 && name == "changed" {
				source = strings.Replace(source, "if !cachehelper.Valid", `if cachehelper.Valid { t.Fatal("changed bytes executed") }; if !cachehelper.Valid`, 1)
			}
			path := filepath.Join(w.Directory, "playbooks", name, "cache_test.go")
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(audit, nil, 0644); err != nil {
			t.Fatal(err)
		}
		if err := w.Check(context.Background()); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(audit)
		if err != nil {
			t.Fatal(err)
		}
		if attempt > 0 {
			if !strings.Contains(string(data), "fixture-diagnostics/playbooks/unchanged\t(cached)") {
				t.Fatalf("unchanged exact package was re-executed in a fresh workspace:\n%s", data)
			}
		}
		if attempt == 2 && !strings.Contains(strings.Join(w.advisoryTestErrors, "\n"), "changed bytes executed") {
			t.Fatalf("changed package reused stale success: %v", w.advisoryTestErrors)
		}
		if attempt == 3 && !strings.Contains(strings.Join(w.advisoryTestErrors, "\n"), "changed dependency executed") {
			t.Fatalf("changed transitive dependency reused stale success: %v", w.advisoryTestErrors)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
