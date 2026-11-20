//go:build integration

package bench

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A rule about a combination of spec fields refuses the plan at `plan add` and `--dry-run` even when one of the fields
// comes from an artifact edge: p2's operator.seed is the p1->p2 artifact and the plan gave it no baseline.
func TestPlanAddRefusesSeedWithoutBaselineNamingItemAndRule(t *testing.T) {
	l := newFlab(t, "h1=c1")
	tpl := func(extra string) string {
		return `{"app": "examples/shop", "operator": {"hooks": "test", "revision": "abcdef0"` + extra + `}, "namespace": "app1",
 "faults": {"instances": "instances.json", "ids": ["a@x"]}, "sequences": [[1]], "run": {"window": "1s"}, "ready_for": "100ms",
 "ready_timeout": "1s", "cap": "10m", "lanes": 1, "hosts": ["h1"], "pause_after_wave": false}`
	}
	must(t, os.WriteFile(filepath.Join(l.specs, "p1.json"), []byte(tpl("")), 0o644))
	must(t, os.WriteFile(filepath.Join(l.specs, "p2.json"), []byte(tpl(`, "seed": "${artifact.rec}"`)), 0o644))
	planFile := filepath.Join(l.specs, "plan.yaml")
	must(t, os.WriteFile(planFile, []byte(`plan: seedless
pins: {workload: fuzzops@0b528ac, subject: test@abcdef0}
arms: [{name: p1, spec: p1.json}, {name: p2, spec: p2.json}]
items: [a]
artifacts: {rec: {path: "${run}/run.json"}}
edges: [{from: p1, to: p2, artifact: rec}]
`), 0o644))
	for _, c := range []struct {
		name string
		f    func() error
	}{
		{"dry-run", func() error { return PlanDryRun(l.root, planFile, false, io.Discard) }},
		{"add", func() error { return PlanAdd(l.root, planFile, io.Discard) }},
	} {
		err := c.f()
		if err == nil || !strings.Contains(err.Error(), "arm p2, item a") || !strings.Contains(err.Error(), "operator.seed needs operator.baseline") {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if fileExists(filepath.Join(string(l.root), "plans", "seedless")) {
		t.Error("a refused plan was stored")
	}
}
