//go:build integration

package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Eight `add` calls of one experiment name at once: exactly one wins, the others are refused, and what is on disk is
// the winner's, whole.
func TestConcurrentAddsOfOneExperimentLeaveOneWholeExperiment(t *testing.T) {
	l := newFlab(t, "h1=c1")
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		spec := fmt.Sprintf("name: e1\napp: examples/shop\nfuzzops: 0b528ac\noperator: {builtin: none}\nnamespace: app1\nfaults:\n  instances: instances.json\n  ids: [a@x]\nsequences: [[%d]]\nrun: {window: 1s}\nready_for: 100ms\nready_timeout: 1s\ncap: 10m\nlanes: 1\nhosts: [h1]\nretries: 0\npause_after_wave: false\n", i+1)
		p := filepath.Join(l.specs, fmt.Sprintf("e1-%d.yaml", i))
		must(t, os.WriteFile(p, []byte(spec), 0o644))
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = Add(context.Background(), l.root, p, os.Stderr) }()
	}
	wg.Wait()
	wins := 0
	for _, e := range errs {
		if e == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("%d adds of the same experiment succeeded: %v", wins, errs)
	}
	x := l.exp("e1")
	if x.Spec == nil || x.SpecErr != "" {
		t.Fatalf("experiment e1 is damaged: spec %v err %q", x.Spec, x.SpecErr)
	}
	if got, want := x.Lock.Spec.Sequences[0][0], x.Spec.Sequences[0][0]; got != want {
		t.Fatalf("lock (sequence %d) and spec (sequence %d) are from different adds", got, want)
	}
}

// Concurrent adds of different experiments and of one plan: every different one lands, the same plan lands once.
func TestConcurrentPlanAddsLeaveOnePlanWithItsItemsOnce(t *testing.T) {
	l := newFlab(t, "h1=c1")
	tpl := `{"app": "examples/shop", "operator": {"hooks": "test"}, "namespace": "app1", "faults": {"instances": "instances.json", "ids": ["${item.fault}"]}, "sequences": [[1]], "run": {"window": "1s"}, "ready_for": "100ms", "ready_timeout": "1s", "cap": "10m", "lanes": 1, "hosts": ["h1"], "pause_after_wave": false}`
	must(t, os.WriteFile(filepath.Join(l.specs, "arm.json"), []byte(tpl), 0o644))
	planFile := filepath.Join(l.specs, "plan.yaml")
	must(t, os.WriteFile(planFile, []byte("plan: race\npins: {workload: fuzzops@0b528ac, subject: test@abcdef0}\narms: [{name: one, spec: arm.json}]\nitems: [{id: a, vars: {fault: \"a@x\"}}, {id: b, vars: {fault: \"b@y\"}}]\nparallel: 2\nisolation: one-way-ok\n"), 0o644))
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = PlanAdd(l.root, planFile, os.Stderr) }()
	}
	wg.Wait()
	wins := 0
	for _, e := range errs {
		if e == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("%d adds of one plan succeeded: %v", wins, errs)
	}
	var out strings.Builder
	must(t, PlanStatus(l.root, "race", true, &out))
	if strings.Count(out.String(), `"node": "one/a/r1"`) != 1 || strings.Count(out.String(), `"node": "one/b/r1"`) != 1 {
		t.Fatalf("plan status after the race:\n%s", out.String())
	}
}
