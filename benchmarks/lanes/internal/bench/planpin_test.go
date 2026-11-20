//go:build integration

package bench

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// `plan add` and `plan add --dry-run` run the same add-time pin check as `add`: a plan whose fuzzops pin is on a host
// with an unstamped binary is refused, naming the host, and nothing is stored.
func TestPlanAddAndDryRunRefuseAnUnstampedBinary(t *testing.T) {
	l := newFlab(t, "h1=c1")
	bad := filepath.Join(l.dir, "badfuzzops")
	must(t, os.MkdirAll(bad, 0o755))
	if out, err := execCmd("cp", "-r", testdata("fakefuzzops")+"/.", bad); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	script := filepath.Join(bad, "bin", "fuzzops")
	b, _ := os.ReadFile(script)
	must(t, os.WriteFile(script, []byte(strings.Replace(string(b), `{"revision":"0b528ac0123456","measure_version":12}`, `{"revision":"unknown","modified":true,"measure_version":12}`, 1)), 0o755))
	inv, _ := os.ReadFile(l.root.path("hosts.yaml"))
	must(t, os.WriteFile(l.root.path("hosts.yaml"), []byte(strings.Replace(string(inv), testdata("fakefuzzops"), bad, -1)), 0o644))

	must(t, os.WriteFile(filepath.Join(l.specs, "t.json"), []byte(`{"app": "examples/shop", "operator": {"builtin": "none"}, "namespace": "app1",
 "faults": {"instances": "instances.json", "ids": ["a@x"]}, "sequences": [[1]], "run": {"window": "1s"}, "ready_for": "100ms",
 "ready_timeout": "1s", "cap": "10m", "lanes": 1, "hosts": ["h1"], "pause_after_wave": false}`), 0o644))
	planFile := filepath.Join(l.specs, "plan.yaml")
	must(t, os.WriteFile(planFile, []byte("plan: pinned\npins: {workload: fuzzops@0b528ac}\narms: [{name: one, spec: t.json}]\nitems: [a]\n"), 0o644))
	for name, f := range map[string]func() error{
		"add":     func() error { return PlanAdd(l.root, planFile, io.Discard) },
		"dry-run": func() error { return PlanDryRun(l.root, planFile, false, io.Discard) },
	} {
		if err := f(); err == nil || !strings.Contains(err.Error(), "h1") || !strings.Contains(err.Error(), "modified checkout") {
			t.Errorf("%s with an unstamped binary: %v", name, err)
		}
	}
	if fileExists(filepath.Join(string(l.root), "plans", "pinned")) {
		t.Error("a refused plan was stored")
	}
}
