//go:build integration

package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// planLab writes experiments straight to disk (a lock, a spec, run files): the backend only reads them.
type planLab struct {
	t    *testing.T
	root Root
	b    *PlanBackend
}

func newPlanLab(t *testing.T) *planLab {
	root := Root(t.TempDir())
	return &planLab{t: t, root: root, b: &PlanBackend{Root: root}}
}

func (l *planLab) experiment(name string, runs map[string]RunFile) {
	l.t.Helper()
	s := Spec{Name: name, Faults: Faults{IDs: []string{"f1"}}, Sequences: [][]int{{1}}, Retries: 0}
	if err := writeJSON(l.root.LockPath(name), Lock{Spec: s, Added: time.Now()}); err != nil {
		l.t.Fatal(err)
	}
	if err := writeJSON(l.root.SpecPath(name), s); err != nil {
		l.t.Fatal(err)
	}
	for id, rf := range runs {
		rf.ID, rf.Experiment, rf.Fault, rf.Sequence, rf.Position, rf.Attempt = id, name, "f1", 1, 1, 1
		if err := writeJSON(filepath.Join(l.root.RunDir(name, id), "run.json"), rf); err != nil {
			l.t.Fatal(err)
		}
	}
}

func TestPlanBackendObserveMapsExperimentsToPhases(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	end := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	l.experiment("q", nil)
	l.experiment("ok", map[string]RunFile{"r1": {Status: StatusDone, Started: end.Add(-time.Hour), Ended: end}})
	l.experiment("oom", map[string]RunFile{"r1": {Status: StatusInvalid, Reason: "oom killed: kubelet", Started: end.Add(-time.Hour), Ended: end}})
	for exp, want := range map[string]struct{ phase, reason string }{
		"q": {plan.PhaseQueued, ""}, "ok": {plan.PhaseDone, ""}, "oom": {plan.PhaseFailed, "oom-killed"}, "none": {plan.PhaseAbsent, ""},
	} {
		got, err := l.b.Observe(ctx, exp)
		if err != nil || got.Phase != want.phase || got.Reason != want.reason {
			t.Errorf("%s: %+v %v, want %+v", exp, got, err, want)
		}
	}
	if got, _ := l.b.Observe(ctx, "ok"); !got.Ended.Equal(end) {
		t.Errorf("ended %v", got.Ended)
	}
}

func TestPlanBackendSubmitAdoptsAnExistingExperiment(t *testing.T) {
	l := newPlanLab(t)
	l.experiment("x", nil)
	if err := l.b.Submit(context.Background(), plan.Job{Exp: "x"}); err != plan.ErrExists {
		t.Fatalf("Submit = %v, want ErrExists", err)
	}
}

func TestPlanBackendExportAndVerify(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	l.experiment("p1", map[string]RunFile{"r1": {Status: StatusDone, Ended: time.Now()}})
	mem := filepath.Join(l.root.RunDir("p1", "r1"), "operator", "learned-baseline")
	a, err := l.b.Export(ctx, plan.Source{Exp: "p1"}, "memory", "${run}/operator/learned-baseline")
	if err != nil || a.Path != mem {
		t.Fatalf("Export = %+v %v", a, err)
	}
	if v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, plan.VerifyExists); v.OK {
		t.Fatalf("missing artifact verified: %+v", v)
	}
	if err := os.MkdirAll(mem, 0o755); err != nil {
		t.Fatal(err)
	}
	if v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, plan.VerifyExists); v.OK {
		t.Fatal("empty directory verified")
	}
	if err := os.WriteFile(filepath.Join(mem, "baseline.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, plan.VerifyExists); !v.OK {
		t.Fatalf("good artifact refused: %+v", v)
	}
	// A command verifies with the artifact in its environment; its last line is the reason it failed.
	if v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, "cmd:test -f $ARTIFACT_PATH/baseline.json"); !v.OK {
		t.Fatalf("cmd verify: %+v", v)
	}
	v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, "cmd:echo head mismatch >&2; exit 1")
	if v.OK || !strings.Contains(v.Detail, "head mismatch") {
		t.Fatalf("cmd verify of a bad artifact: %+v", v)
	}
	// subject with no hooks configured falls back to exists.
	if v, _ := l.b.Verify(ctx, plan.Source{Exp: "p1"}, a, plan.VerifySubject); !v.OK {
		t.Fatalf("subject verify without hooks: %+v", v)
	}
}

func TestPlanBackendStepRunsDetachedAndIsObservedFromItsRecords(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "steps", "step_merge")
	job := plan.StepJob{Plan: "tp", ID: "step:merge", Dir: dir, Timeout: time.Minute,
		Run:    `mkdir -p "$STEP_DIR/merged" && echo "$LANES_INPUTS" > "$STEP_DIR/merged/inputs"`,
		Inputs: []plan.StepInput{{Node: "p1/a/r1", Exp: "e"}}}
	if err := l.b.StartStep(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := l.b.StartStep(ctx, job); err != nil { // a restart repeats the start
		t.Fatal(err)
	}
	var obs plan.StepObservation
	for i := 0; i < 100 && obs.Phase != plan.PhaseDone; i++ {
		obs, _ = l.b.ObserveStep(ctx, dir)
		time.Sleep(50 * time.Millisecond)
	}
	if obs.Phase != plan.PhaseDone {
		t.Fatalf("step = %+v", obs)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "merged", "inputs")); !strings.Contains(string(b), "p1/a/r1") {
		t.Fatalf("inputs: %q", b)
	}

	bad := filepath.Join(t.TempDir(), "steps", "step_bad")
	_ = l.b.StartStep(ctx, plan.StepJob{Plan: "tp", ID: "step:bad", Dir: bad, Timeout: time.Minute, Run: "echo no merge possible; exit 3"})
	for i := 0; i < 100; i++ {
		if obs, _ = l.b.ObserveStep(ctx, bad); obs.Phase == plan.PhaseFailed {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if obs.Phase != plan.PhaseFailed || !strings.Contains(obs.Detail, "exit 3") || !strings.Contains(obs.Detail, "no merge possible") {
		t.Fatalf("failed step = %+v", obs)
	}
	if obs, _ := l.b.ObserveStep(ctx, filepath.Join(t.TempDir(), "never")); obs.Phase != plan.PhaseAbsent {
		t.Fatalf("unstarted step = %+v", obs)
	}
}

// A plan on the real manager: the first arm of each item runs through the fake fuzzops, its run record is the
// artifact the second arm takes, and the manager's own passes drive the plan to its end.
func TestPlanRunsOnTheManagerEndToEnd(t *testing.T) {
	l := newLab(t)
	rev := strings.TrimSuffix(strings.TrimPrefix(l.hooked, "{hooks: test, revision: "), ", baseline: base}")
	tpl := fmt.Sprintf(`{"app": "examples/shop", "operator": {"hooks": "test", "baseline": "base", "seed": "${artifact.rec}"},
 "namespace": "app1", "faults": {"instances": "instances.json", "ids": ["${item.fault}"]}, "sequences": [[1]], "run": {"window": "1s"},
 "ready_for": "100ms", "ready_timeout": "%s", "cap": "%s", "lanes": 1, "hosts": ["h1"], "pause_after_wave": false}`, l.ready, l.cap)
	first := strings.Replace(tpl, `, "seed": "${artifact.rec}"`, "", 1)
	must(t, os.WriteFile(filepath.Join(l.specs, "first.json"), []byte(first), 0o644))
	must(t, os.WriteFile(filepath.Join(l.specs, "second.json"), []byte(tpl), 0o644))
	planFile := filepath.Join(l.specs, "plan.yaml")
	must(t, os.WriteFile(planFile, []byte(`plan: e2e
pins: {workload: fuzzops@0b528ac, subject: test@`+rev+`}
arms: [{name: one, spec: first.json}, {name: two, spec: second.json}]
items: [{id: a, vars: {fault: "a@x"}}, {id: b, vars: {fault: "b@y"}}]
parallel: 2
isolation: one-way-ok
artifacts: {rec: {path: "${run}/run.json"}}
edges: [{from: one, to: two, artifact: rec}]
`), 0o644))
	must(t, PlanAdd(l.root, planFile, io.Discard))
	l.until("the plan to complete", func() bool {
		st, err := plan.LoadState(plan.Dir(l.root.PlansDir(), "e2e"))
		must(t, err)
		return st.Complete
	})
	st, _ := plan.LoadState(plan.Dir(l.root.PlansDir(), "e2e"))
	for _, n := range st.Nodes {
		if n.Status != plan.NodeDone {
			t.Errorf("%s = %s %s", n.ID, n.Status, n.Reason)
		}
	}
	// The second arm ran on the first's verified record.
	seed := l.exp(expNameOf(t, st, "two/a/r1")).Lock.Spec.Operator.Seed
	want := l.root.path(st.Nodes[0].Exp, "runs")
	if !strings.HasPrefix(seed, want) || !strings.HasSuffix(seed, "run.json") {
		t.Fatalf("seed = %q, want a run.json under %s", seed, want)
	}
	var out strings.Builder
	must(t, PlanStatus(l.root, "", false, &out))
	if !strings.Contains(out.String(), "complete") {
		t.Fatalf("status:\n%s", out.String())
	}
}

// expNameOf is the experiment of a node in a loaded state.
func expNameOf(t *testing.T, st *plan.State, id string) string {
	t.Helper()
	for _, n := range st.Nodes {
		if n.ID == id {
			return n.Exp
		}
	}
	t.Fatalf("no node %s", id)
	return ""
}
