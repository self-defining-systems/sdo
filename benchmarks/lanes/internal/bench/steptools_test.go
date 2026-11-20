//go:build integration

package bench

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The tools check must run in exactly the environment a step runs in. A build on a reserved host failed with exit 127
// ("kind" not found) although preflight had passed: the check added PATH directories no step has, and did not go through
// the lane-exec wrapper every step of a reserved host starts under.

func fakeTools(t *testing.T, dir string, tools ...string) {
	t.Helper()
	must(t, os.MkdirAll(dir, 0o755))
	for _, tool := range tools {
		must(t, os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"), 0o755))
	}
}

func runProbe(t *testing.T, script, home string) string {
	t.Helper()
	cmd := exec.Command("sh", "-c", script)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + home}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	return string(out)
}

func TestToolsProbeAddsNoPathDirectoryASepDoesNotHave(t *testing.T) {
	root := t.TempDir()
	bin, home := filepath.Join(root, "bin"), filepath.Join(root, "home")
	fakeTools(t, bin, "kubectl", "docker", "ip")
	fakeTools(t, filepath.Join(home, ".local", "bin"), "kind") // where the old check also looked; no step does
	h := Host{Env: map[string]string{"PATH": bin + ":/usr/bin:/bin"}}
	out := runProbe(t, toolsProbe(h, ""), home)
	if !strings.Contains(out, "missing kind") {
		t.Fatalf("kind is only in a directory no step searches, so the probe must say it is missing, got %q", out)
	}
}

func TestToolsProbeCarriesTheWorkloadEnvOfTheSpecsWorkload(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	fakeTools(t, bin, "kubectl", "docker", "ip", "kind")
	h := Host{WorkloadEnv: map[string]map[string]string{"wl": {"PATH": bin + ":/usr/bin:/bin"}}}
	if out := runProbe(t, toolsProbe(h, "wl"), root); strings.Contains(out, "missing") {
		t.Fatalf("the workload's own PATH is the step's PATH for that workload: %q", out)
	}
	if out := runProbe(t, toolsProbe(h, "other"), root); !strings.Contains(out, "missing kind") {
		t.Fatalf("another workload's steps do not get it: %q", out)
	}
}

// On a reserved host every step starts under the helper's lane-exec, which sets the step's PATH itself.
func TestToolsProbeRunsUnderLaneExecOnAReservedHost(t *testing.T) {
	h := Host{Clusters: []string{"c1"}, Reserve: &HostReserve{CPUs: "2-3"}, Env: map[string]string{"A": "1"}}
	got := toolsProbe(h, "")
	if !strings.HasPrefix(got, "sudo -n --preserve-env "+HelperPath+" lane-exec c1 -- sh -c ") || !strings.Contains(got, "export A='1'") {
		t.Fatalf("probe = %q", got)
	}
	if plain := toolsProbe(Host{Clusters: []string{"c1"}}, ""); strings.Contains(plain, "lane-exec") {
		t.Fatalf("an unreserved host's steps are not wrapped: %q", plain)
	}
}

func TestPlanAddNamesAMissingToolOfTheStepEnvironment(t *testing.T) {
	inv, err := loadInv(t, `hosts:
  h:
    local: true
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 1
    max_load5: 10
    clusters: [c1]
checkouts:
  fuzzops: {abcdef0: {h: /fz}}
`)
	must(t, err)
	run := func(ctx context.Context, host, script string) (string, error) { return "missing kind\n", nil }
	got := inv.ToolProblems(context.Background(), Spec{Fuzzops: "abcdef0", Hosts: []string{"h"}}, run)
	if len(got) != 1 || got[0] != "host h: kind not found on the step PATH" {
		t.Fatalf("problems = %q", got)
	}
}
