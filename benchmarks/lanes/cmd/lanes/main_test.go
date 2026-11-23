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

// `--dry-run` belongs to `plan add`; every flag belongs to some verbs only; `lanes add FILE --dry-run` used to ignore the flag and store the experiment (three
// pool experiments of 18 lanes were added that way and billed a vcluster host for 35 minutes).
func TestAVerbRefusesAFlagOfAnotherVerbInsteadOfIgnoringIt(t *testing.T) {
	t.Setenv("FUZZOPS_BENCH_ROOT", "")
	root := t.TempDir()
	spec := filepath.Join(root, "spec.json")
	if err := os.WriteFile(spec, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	for verb, args := range map[string][]string{
		"add":    {spec, "--dry-run"},
		"pause":  {"x", "--keep"},
		"resume": {"x", "--json"},
		"status": {"--keep"},
		"gc":     {"--tsv"},
	} {
		var code int
		_, errs := capture(t, func() { code = run(verb, append(args, "--root", root)) })
		if code == 0 || !strings.Contains(errs, "unknown flag") {
			t.Errorf("%s %v: exit %d, stderr %q; want a refusal naming the flag", verb, args, code, errs)
		}
	}
	_, errs := capture(t, func() { run("add", []string{spec, "--dry-run", "--root", root}) })
	if !strings.Contains(errs, "plan add") {
		t.Errorf("add --dry-run does not point at plan add: %q", errs)
	}
	if _, err := os.Stat(filepath.Join(root, "bench.lock")); err == nil {
		t.Error("add --dry-run reached the add (it took the add lock)")
	}
}
