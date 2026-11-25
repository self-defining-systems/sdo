//go:build integration

package bench

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// `lanes plan remove` cleans up a plan's experiments and state and frees its name, so a corrected plan can be added
// under the same name (it used to take stopping the manager, `lanes remove` and moving plans/<name> aside by hand).
func TestPlanRemoveFreesTheNameForACorrectedPlan(t *testing.T) {
	l := newLab(t)
	rev := strings.TrimSuffix(strings.TrimPrefix(l.hooked, "{hooks: test, revision: "), ", baseline: base, settings: {startup_bound: 0s}}")
	must(t, os.WriteFile(filepath.Join(l.specs, "t.json"), []byte(`{"app": "examples/shop", "operator": {"hooks": "test", "baseline": "base"},
 "namespace": "app1", "faults": {"instances": "instances.json", "ids": ["${item.fault}"]}, "sequences": [[1]], "run": {"window": "1s"},
 "ready_for": "100ms", "ready_timeout": "`+l.ready+`", "cap": "`+l.cap+`", "lanes": 1, "hosts": ["h1"], "pause_after_wave": false}`), 0o644))
	planFile := filepath.Join(l.specs, "plan.yaml")
	text := "plan: rm\npins: {workload: fuzzops@0b528ac, subject: test@" + rev + "}\narms: [{name: one, spec: t.json}]\nitems: [{id: a, vars: {fault: \"a@x\"}}]\nisolation: one-way-ok\n"
	must(t, os.WriteFile(planFile, []byte(text), 0o644))
	must(t, PlanAdd(l.root, planFile, io.Discard))
	dir := plan.Dir(l.root.PlansDir(), "rm")
	exp := plan.ExpName("rm", "one/a/r1", 1)
	l.until("the plan's experiment to be released", func() bool {
		_, err := os.Stat(l.root.path(exp))
		return err == nil
	})

	// Same name now: refused.
	if err := PlanAdd(l.root, planFile, io.Discard); err == nil {
		t.Fatal("a live plan's name was reused")
	}
	var out strings.Builder
	must(t, PlanRemove(l.root, "rm", false, &out))
	l.until("the plan to be cleaned up", func() bool {
		_, err := os.Stat(dir)
		return err != nil
	})
	if _, err := os.Stat(l.root.path(exp)); err == nil {
		t.Error("the experiment directory is still under the root, so the same name would adopt it")
	}
	if _, err := os.Stat(l.root.SpecPath(exp)); err == nil {
		t.Error("the experiment spec is still under specs/")
	}
	arch, _ := filepath.Glob(filepath.Join(l.root.PlansDir(), ".removed", "rm-*", "lock.json"))
	if len(arch) != 1 {
		t.Fatalf("the plan state is archived, not deleted: %v", arch)
	}
	// The command says where the results are: the directory is gone from its usual path.
	if want := filepath.Join(l.root.PlansDir(), ".removed", "rm-"); !strings.Contains(out.String(), want) || !strings.Contains(out.String(), "kept") {
		t.Errorf("the command does not say that the results are kept, and where (%s): %q", want, out.String())
	}
	for _, n := range plan.Names(l.root.PlansDir()) {
		t.Errorf("plan %s still listed", n)
	}
	// The corrected plan takes the name; its experiment is new.
	must(t, PlanAdd(l.root, planFile, io.Discard))
	l.until("the new plan's experiment", func() bool {
		x, err := l.root.LoadExperiment(exp)
		return err == nil && x.Spec != nil
	})
}
