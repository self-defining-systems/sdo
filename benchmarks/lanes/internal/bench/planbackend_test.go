//go:build integration

package bench

import (
	"context"
	"encoding/json"
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
stages: {post-inject: 1ms}
alarms: {factor: 1}
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
	l.m.planGrading.Wait() // grading jobs write into the root: let them end before it goes
	var rows []plan.Row
	l.until("a row per run", func() bool {
		rows, _, _ = plan.Results(plan.Dir(l.root.PlansDir(), "e2e"), plan.RowFilter{})
		return len(rows) == 4
	})
	l.m.planGrading.Wait()
	for _, r := range rows {
		if r.Status != "done" || r.Run == "" || r.Source.WorkloadObserved == "" || !r.Source.OK {
			t.Errorf("row %+v", r)
		}
	}
	// Every run took longer than the 1 ms minimum the plan declared for its post-inject stage: one stage alarm per run
	// in alarms.jsonl, and `status` shows them.
	if got := RecentAlarms(l.root, time.Hour, time.Now()); len(got) != 4 || got[0].Alarm != plan.AlarmStage {
		t.Fatalf("alarms %+v", got)
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

func TestPlanBackendStagesAndCurrentStage(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	added := time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
	start, inject, end := added.Add(10*time.Minute), added.Add(25*time.Minute), added.Add(40*time.Minute)
	l.experiment("ok", map[string]RunFile{"r1": {Status: StatusDone, Started: start, InjectAt: &inject, Ended: end}})
	must(t, writeJSON(l.root.LockPath("ok"), Lock{Spec: Spec{Name: "ok", Faults: Faults{IDs: []string{"f1"}}, Sequences: [][]int{{1}}}, Added: added}))
	obs, _ := l.b.Observe(ctx, "ok")
	want := map[string]time.Duration{StageQueue: 10 * time.Minute, StagePreInject: 15 * time.Minute, StagePostInject: 15 * time.Minute}
	for k, d := range want {
		if obs.Stages[k] != d {
			t.Errorf("stage %s = %v want %v (all %v)", k, obs.Stages[k], d, obs.Stages)
		}
	}
	// A queued experiment is in the queue stage since it was added; one on a lane is in its step's phase.
	l.experiment("q", nil)
	must(t, writeJSON(l.root.LockPath("q"), Lock{Spec: Spec{Name: "q", Faults: Faults{IDs: []string{"f1"}}, Sequences: [][]int{{1}}}, Added: added}))
	if obs, _ := l.b.Observe(ctx, "q"); obs.Stage != StageQueue || !obs.StageSince.Equal(added) {
		t.Errorf("queued: %+v", obs)
	}
	since := added.Add(time.Hour)
	must(t, writeJSON(l.root.LanePath("c1"), LaneFile{Lane: "c1", Host: "h1", State: LaneRunning, Experiment: "q", Seq: &Seq{Fault: "f1", Q: 1},
		Step: &StepRef{ID: "s1", Kind: "run", Started: since, Phase: &Phase{Name: PhaseGate, Since: since}}}))
	if obs, _ := l.b.Observe(ctx, "q"); obs.Phase != plan.PhaseRunning || obs.Stage != PhaseGate || !obs.StageSince.Equal(since) {
		t.Errorf("on a lane: %+v", obs)
	}
}

func TestAlarmLogWritesTheFileAndRunsTheHook(t *testing.T) {
	root := Root(t.TempDir())
	out := filepath.Join(t.TempDir(), "hook.out")
	log := &AlarmLog{Root: root, Hook: `echo "$ALARM_KIND $ALARM_PLAN $ALARM_SUBJECT" > ` + out}
	a := plan.Alarm{At: time.Now().UTC(), Plan: "tp", Alarm: plan.AlarmIdleLane, Key: "idle-lane:h1/c1", Subject: "h1/c1", Detail: "free for 20m"}
	log.Raise(a)
	if b, _ := os.ReadFile(out); strings.TrimSpace(string(b)) != "idle-lane tp h1/c1" {
		t.Fatalf("hook saw %q", b)
	}
	// A failing hook does not stop anything; the line is still in the file.
	(&AlarmLog{Root: root, Hook: "exit 7"}).Raise(a)
	got := RecentAlarms(root, time.Hour, time.Now())
	if len(got) != 2 || got[0].Key != a.Key {
		t.Fatalf("alarms.jsonl: %+v", got)
	}
	var w strings.Builder
	printAlarms(root, &w)
	if !strings.Contains(w.String(), "idle-lane") || !strings.Contains(w.String(), "free for 20m") {
		t.Fatalf("status alarms:\n%s", w.String())
	}
}

func TestRecentAlarmsSkipsDamagedAndOldLines(t *testing.T) {
	root := Root(t.TempDir())
	now := time.Now().UTC()
	old, _ := json.Marshal(plan.Alarm{At: now.Add(-48 * time.Hour), Alarm: "stage", Key: "old"})
	fresh, _ := json.Marshal(plan.Alarm{At: now.Add(-time.Hour), Alarm: "stage", Key: "fresh"})
	drift := `{"at":"` + now.Format(time.RFC3339) + `","host":"h1","alarm":"reservation-drift","detail":"x"}`
	must(t, os.WriteFile(root.path("alarms.jsonl"), []byte(string(old)+"\n\nnot json\n"+string(fresh)+"\n"+drift+"\n{\"at\":"), 0o644))
	got := RecentAlarms(root, 24*time.Hour, now)
	if len(got) != 2 || got[0].Key != "fresh" || got[1].Alarm != "reservation-drift" {
		t.Fatalf("got %+v", got)
	}
	var w strings.Builder
	printAlarms(Root(t.TempDir()), &w) // no file: nothing printed
	if w.Len() != 0 {
		t.Fatalf("printed %q", w.String())
	}
}

func TestPlanBackendCapacity(t *testing.T) {
	l := newLab(t)
	l.add("e1", "{builtin: noop}", "a@x, b@y", "[[1]]", 0)
	c, err := (&PlanBackend{Root: l.root}).Capacity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c.Waiting != 2 {
		t.Fatalf("waiting %d, want the 2 queued runs", c.Waiting)
	}
}

func TestPlanBackendRunDataReadsIdentityAndTheRunsGradeLine(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	end := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	l.experiment("e", map[string]RunFile{"r1": {Status: StatusDone, Lane: "c1", Host: "h1", OperatorLabel: "exec:e-r1", Ended: end,
		Fuzzops:   Version{Revision: "46526567aa4f8ad4c"},
		Isolation: &Isolation{Mode: "two-way", Reserved: true, OOMKills: 0, CPUs: "4-11", Memory: "20Gi"}}})
	must(t, writeJSON(filepath.Join(l.root.RunDir("e", "r1"), "result.json"), Result{Prepared: &Prepared{Version: "4844e69dba6c", AppPack: "/co/examples/gitlab"}}))
	dir := l.root.path("e", "results")
	must(t, os.MkdirAll(dir, 0o755))
	// A blank line (an experiment with no graded run writes one), a damaged line, someone else's grade, then ours.
	must(t, os.WriteFile(filepath.Join(dir, "runs.jsonl"), []byte("\nnot json\n{\"operator\":\"exec:other\",\"valid\":false}\n{\"operator\":\"exec:e-r1\",\"valid\":true,\"bad_s\":4}\n"), 0o644))
	rd, err := l.b.RunData(ctx, "e")
	if err != nil {
		t.Fatal(err)
	}
	if rd.Run != "r1" || rd.Host != "h1" || rd.WorkloadObserved != "46526567aa4f8ad4c" || rd.SubjectObserved != "4844e69dba6c" || rd.App != "/co/examples/gitlab" ||
		rd.Isolation.Mode != "two-way" || rd.Isolation.CPUs != "4-11" || rd.Pending || !strings.Contains(string(rd.Grade), `"bad_s":4`) {
		t.Fatalf("RunData = %+v grade %s", rd, rd.Grade)
	}
}

func TestPlanBackendRunDataWithAnEmptyGradeFileIsAnErrorNotACrash(t *testing.T) {
	l := newPlanLab(t)
	ctx := context.Background()
	l.experiment("e", map[string]RunFile{"r1": {Status: StatusDone, OperatorLabel: "exec:e-r1", Ended: time.Now()}})
	dir := l.root.path("e", "results")
	must(t, os.MkdirAll(dir, 0o755))
	must(t, os.WriteFile(filepath.Join(dir, "runs.jsonl"), []byte("\n"), 0o644))
	must(t, os.WriteFile(filepath.Join(dir, "graded"), []byte("ok"), 0o644))
	rd, err := l.b.RunData(ctx, "e")
	if err != nil || rd.Grade != nil || !strings.Contains(rd.GradeError, "no graded line for operator exec:e-r1") {
		t.Fatalf("RunData = %+v %v", rd, err)
	}
	// Grading in progress: pending, and the marker is not started twice.
	must(t, os.Remove(filepath.Join(dir, "graded")))
	must(t, os.WriteFile(filepath.Join(dir, "grading"), []byte("now"), 0o644))
	if rd, _ := l.b.RunData(ctx, "e"); !rd.Pending {
		t.Fatalf("grading in progress should be pending: %+v", rd)
	}
	// A failed grade is reported, not retried for ever.
	must(t, os.Remove(filepath.Join(dir, "grading")))
	must(t, os.WriteFile(filepath.Join(dir, "grade.error"), []byte("fuzzops report: exit 1\nno such pack"), 0o644))
	if rd, _ := l.b.RunData(ctx, "e"); !strings.Contains(rd.GradeError, "no such pack") {
		t.Fatalf("RunData = %+v", rd)
	}
}

func TestPlanBackendRunDataOfAnInvalidRunHasIdentityAndNoGrade(t *testing.T) {
	l := newPlanLab(t)
	l.experiment("e", map[string]RunFile{"r1": {Status: StatusInvalid, Reason: "oom", Ended: time.Now(), Fuzzops: Version{Revision: "abc1234"}}})
	rd, err := l.b.RunData(context.Background(), "e")
	if err != nil || rd.Run != "r1" || rd.WorkloadObserved != "abc1234" || rd.Grade != nil || rd.Pending {
		t.Fatalf("RunData = %+v %v", rd, err)
	}
}

func TestPlanResultsCommandOnEmptyAndPartialFiles(t *testing.T) {
	root := Root(t.TempDir())
	dir := plan.Dir(root.PlansDir(), "tp")
	must(t, os.MkdirAll(dir, 0o755))
	must(t, os.WriteFile(filepath.Join(dir, "lock.json"), []byte("{}"), 0o644))
	var out, errs strings.Builder
	// No results file at all: just the header.
	must(t, PlanResults(root, "tp", ResultsOptions{TSV: true}, &out, &errs))
	if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 1 || !strings.HasPrefix(lines[0], "plan\tnode") {
		t.Fatalf("empty: %q", out.String())
	}
	// An empty file, then a partial one (a torn last line): no crash, rows that parse are kept, the rest counted.
	must(t, os.WriteFile(filepath.Join(dir, "results.jsonl"), nil, 0o644))
	out.Reset()
	must(t, PlanResults(root, "tp", ResultsOptions{TSV: true}, &out, &errs))
	row := `{"plan":"tp","node":"p1/a/r1","arm":"p1","item":"a","rep":1,"attempt":1,"exp":"e","status":"done","valid":true,"grade":{"valid":true,"recovered":true},"source":{"ok":true},"isolation":{"mode":"two-way"}}`
	must(t, os.WriteFile(filepath.Join(dir, "results.jsonl"), []byte(row+"\n{\"plan\":\"tp\",\"node\":\"p1/b/r1\",\"att"), 0o644))
	out.Reset()
	must(t, PlanResults(root, "tp", ResultsOptions{TSV: true, GradeFields: "recovered"}, &out, &errs))
	if !strings.Contains(out.String(), "p1/a/r1") || !strings.Contains(out.String(), "grade.recovered") || !strings.Contains(errs.String(), "skipped 1 damaged") {
		t.Fatalf("partial: %q / %q", out.String(), errs.String())
	}
	out.Reset()
	must(t, PlanResults(root, "tp", ResultsOptions{}, &out, &errs))
	if !strings.HasPrefix(out.String(), `{"plan":"tp","node":"p1/a/r1"`) {
		t.Fatalf("jsonl: %q", out.String())
	}
	if err := PlanResults(root, "nope", ResultsOptions{}, &out, &errs); err == nil {
		t.Fatal("unknown plan accepted")
	}
}
