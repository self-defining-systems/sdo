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
	for attempt := 0; attempt < 3; attempt++ {
		w := acceptanceFixture(t)
		for _, name := range []string{"changed", "unchanged"} {
			source := "package probe\nimport \"testing\"\nfunc TestCacheIdentity(t *testing.T) {}\n"
			if attempt == 2 && name == "changed" {
				source = strings.Replace(source, "{}", `{ t.Fatal("changed bytes executed") }`, 1)
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
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
