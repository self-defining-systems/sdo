//go:build integration

package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// capture runs f and returns what it wrote to stdout and stderr.
func capture(t *testing.T, f func()) (out, errs string) {
	t.Helper()
	oo, oe := os.Stdout, os.Stderr
	ro, wo, _ := os.Pipe()
	re, we, _ := os.Pipe()
	os.Stdout, os.Stderr = wo, we
	f()
	os.Stdout, os.Stderr = oo, oe
	wo.Close()
	we.Close()
	bo, _ := io.ReadAll(ro)
	be, _ := io.ReadAll(re)
	return string(bo), string(be)
}

// Every verb takes --root, before or after its other arguments, like the fuzzops-bench verbs did; the host verb used
// to read the root before parsing its flags, so `lanes host status --root R` failed with "open hosts.yaml".
func TestHostVerbTakesRootFlag(t *testing.T) {
	t.Setenv("FUZZOPS_BENCH_ROOT", "")
	root := t.TempDir()
	hosts := "hosts:\n  h1:\n    local: true\n    staging: /tmp/lanes-test-staging\n    kubeconfigs: /tmp/lanes-test-kube\n    max_builds: 1\n    max_load5: 10\n    clusters: []\n"
	if err := os.WriteFile(filepath.Join(root, "hosts.yaml"), []byte(hosts), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--root", root, "status", "h1"},
		{"status", "h1", "--root", root},
		{"status", "--root=" + root, "h1"},
	} {
		out, errs := capture(t, func() { run("host", args) })
		if strings.Contains(errs, "hosts.yaml") || !strings.Contains(out+errs, "h1") {
			t.Errorf("host %v: stdout %q stderr %q", args, out, errs)
		}
	}
}
