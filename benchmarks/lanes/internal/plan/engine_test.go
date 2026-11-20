package plan

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestPlanRunsInOrderAndChainsOnlyAfterAVerifiedArtifact(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)

	// Two slots: the first two nodes of the order, the pass-1 run and its cold partner. Pass 2 of the same item
	// is third in the order, but its input does not exist yet.
	l.tick(e)
	if got, want := l.fb.nodes(), []string{"p1/a/r1", "cold/a/r1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first release %v want %v", got, want)
	}
	l.tick(e)
	if len(l.fb.nodes()) != 2 {
		t.Fatalf("a full window released more: %v", l.fb.nodes())
	}

	// The cold run ends first: its slot goes to the next ready node in order, not to p2/a/r1 (blocked on p1/a/r1).
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	if got := l.fb.nodes(); got[len(got)-1] != "p1/b/r1" {
		t.Fatalf("slot went to %v, want p1/b/r1 (p2/a/r1 is blocked)", got)
	}

	// The pass-1 run ends and its memory verifies: pass 2 of that item is released ahead of later items' nodes.
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.tick(e)
	if got := l.fb.nodes(); got[len(got)-1] != "p2/a/r1" {
		t.Fatalf("releases %v: want p2/a/r1 right after its input verified", got)
	}
	var spec map[string]any
	b, _ := os.ReadFile(l.fb.subs[len(l.fb.subs)-1].SpecPath)
	if err := json.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	op := spec["operator"].(map[string]any)
	if op["seed"] != "/art/"+l.fb.expOf("p1/a/r1")+"/memory" || op["revision"] != "9b2cadb7f" || spec["fuzzops"] != "4652656" ||
		spec["retries"] != float64(0) || spec["marker"] != "p2-1" || spec["name"] != l.fb.expOf("p2/a/r1") {
		t.Fatalf("rendered spec wrong: %v", spec)
	}
	if got := spec["faults"].(map[string]any)["instances"]; got != l.root+"/inst.json" {
		t.Fatalf("relative path not made absolute against the plan file: %v", got)
	}
}

func TestFailedVerifyBlocksTheDependentRun(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.verdicts["memory@"+exp] = Verdict{OK: false, Detail: "archive head 1111 is not live HEAD 2222"}
	l.fb.finish(exp, PhaseDone, "")
	l.tick(e)

	// The bad artifact fails the source's attempt; the dependent must not run on it, and waits for the rerun.
	st := l.state()
	if p1 := st.node("p1/a/r1"); p1.Status == NodeDone || p1.Attempts[0].Reason != "artifact:memory" {
		t.Fatalf("p1/a/r1 = %s %+v, want its attempt failed with artifact:memory", p1.Status, p1.Attempts)
	}
	for _, n := range l.fb.nodes() {
		if n == "p2/a/r1" {
			t.Fatal("the dependent was released on an unverified artifact")
		}
	}
	// The rerun produces the same bad archive: the second identical failure parks the item, the dependent is skipped.
	l.tick(e)
	exp2 := l.fb.expOf("p1/a/r1")
	if exp2 == exp {
		t.Fatalf("rerun reused %s", exp)
	}
	l.fb.verdicts["memory@"+exp2] = Verdict{OK: false, Detail: "archive head 1111 is not live HEAD 2222"}
	l.fb.finish(exp2, PhaseDone, "")
	l.tick(e)
	st = l.state()
	if p1 := st.node("p1/a/r1"); p1.Status != NodeFailed || st.Parked["a"] != "artifact:memory" {
		t.Fatalf("p1/a/r1 = %s, parked %v", p1.Status, st.Parked)
	}
	if p2 := st.node("p2/a/r1"); p2.Status != NodeSkipped && p2.Status != NodeParked {
		t.Fatalf("p2/a/r1 = %s %q, want it ended without running", p2.Status, p2.Reason)
	}
	for _, n := range l.fb.nodes() {
		if n == "p2/a/r1" {
			t.Fatal("the dependent ran on a bad artifact")
		}
	}
}

func TestArtifactIsVerifiedAgainBeforeTheDependentStarts(t *testing.T) {
	// The world moves between the source's end and the dependent's release (the head advances): the second verify,
	// run at release, fails the source, which runs again.
	l := newLab(t)
	e := l.add(strings.Replace(twopass, "parallel: 2", "parallel: 1", 1))
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	l.fb.finish(exp, PhaseDone, "")
	l.tick(e) // verified; cold/a/r1 takes the one slot
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.fb.verdicts["memory@"+exp] = Verdict{OK: false, Detail: "head moved"}
	l.tick(e)
	st := l.state()
	if st.node("p1/a/r1").Status == NodeDone || st.node("p2/a/r1").Status == NodeSubmitted {
		t.Fatalf("p1 %s, p2 %s: a stale artifact must not chain", st.node("p1/a/r1").Status, st.node("p2/a/r1").Status)
	}
	if st.node("p1/a/r1").Attempts[0].Reason != "artifact:memory" {
		t.Fatalf("attempts %+v", st.node("p1/a/r1").Attempts)
	}
}

func TestRestartMidPlanContinuesFromDisk(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	before := append([]string(nil), l.fb.nodes()...)

	// A new process: a new engine on the same directory, nothing in memory.
	e2 := l.open()
	l.tick(e2)
	if !reflect.DeepEqual(l.fb.nodes(), before) {
		t.Fatalf("restart re-released: %v then %v", before, l.fb.nodes())
	}
	// Crash between Submit and the state write: the state still says waiting; the retry finds the experiment.
	st := l.state()
	n := st.node("p1/b/r1")
	if n.Status != NodeSubmitted {
		t.Fatalf("p1/b/r1 = %s", n.Status)
	}
	n.Status, n.Attempt, n.Exp = NodeWaiting, 0, ""
	if err := writeJSONFile(stateFile(e.Dir), st); err != nil {
		t.Fatal(err)
	}
	l.tick(l.open())
	if got := l.state().node("p1/b/r1"); got.Status != NodeSubmitted || got.Exp != l.fb.expOf("p1/b/r1") {
		t.Fatalf("submission not adopted: %+v", got)
	}
	if len(l.fb.nodes()) != len(before) {
		t.Fatalf("adopting submitted twice: %v", l.fb.nodes())
	}
}

func TestEditedPlanOrTemplateRefusesToRun(t *testing.T) {
	l := newLab(t)
	l.add(twopass)
	dir := l.root + "/plans/tp"
	if err := os.WriteFile(dir+"/templates/p1.json", []byte(`{"name":"other"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, l.fb, l.clock); err == nil || !strings.Contains(err.Error(), "template of arm p1") {
		t.Fatalf("Open = %v", err)
	}
}

func TestPlanNamesAreNeverReused(t *testing.T) {
	l := newLab(t)
	l.add(twopass)
	if _, err := Add(l.root+"/plans", l.root+"/plan.yaml", l.clock, nil); err == nil || !strings.Contains(err.Error(), "never reused") {
		t.Fatalf("second add = %v", err)
	}
}

func TestLockRecordsPinsAndInputs(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	if e.Lock.Pins.Workload.String() != "fuzzops@4652656" || e.Lock.Pins.Subject.String() != "sdo@9b2cadb7f" ||
		e.Lock.Nodes != 12 || !reflect.DeepEqual(e.Lock.Items, []string{"a", "b"}) || len(e.Lock.Templates) != 3 || e.Lock.PlanSHA == "" {
		t.Fatalf("lock = %+v", e.Lock)
	}
}

func TestLaneFreeMergeStepRunsOverDoneNodesAndFeedsLaterArms(t *testing.T) {
	l := newLab(t)
	plan := `
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: p1, spec: tpl-plain.yaml}, {name: p2, spec: tpl.yaml}]
items: [a, b]
parallel: 8
artifacts: {memory: {path: "${run}/mem"}}
steps: [{name: merge, after: [p1], run: "merge-memories", lane: none, out: "${step_dir}/merged"}]
edges: [{from: merge, to: p2, artifact: memory}]
`
	e := l.add(plan)
	l.tick(e)
	if got := l.fb.nodes(); !reflect.DeepEqual(got, []string{"p1/a/r1", "p1/b/r1"}) {
		t.Fatalf("p2 waits for the merge, p1 runs first: %v", got)
	}
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.tick(e)
	if len(l.fb.started) != 0 {
		t.Fatal("merge started before every pass-1 run ended")
	}
	l.fb.finish(l.fb.expOf("p1/b/r1"), PhaseFailed, "oom")
	l.tick(e) // the first oom of item b: p1/b/r1 runs again
	l.fb.finish(l.fb.expOf("p1/b/r1"), PhaseFailed, "oom")
	l.tick(e) // the second parks item b
	if len(l.fb.started) != 1 || len(l.fb.started[0].Inputs) != 1 || l.fb.started[0].Inputs[0].Node != "p1/a/r1" {
		t.Fatalf("merge runs once over the done nodes only: %+v", l.fb.started)
	}
	l.tick(e)
	for _, n := range l.fb.nodes() {
		if strings.HasPrefix(n, "p2/") {
			t.Fatal("p2 released while the merge runs")
		}
	}
	l.fb.steps["step:merge"] = StepObservation{Phase: PhaseDone}
	l.tick(e)
	l.tick(e)
	// Item b is parked (two oom failures): p2/b never runs; p2/a took the merge.
	if got := l.fb.nodes(); got[len(got)-1] != "p2/a/r1" {
		t.Fatalf("after the merge: %v", got)
	}
	st := l.state()
	if st.step("step:merge").Status != StepDone || st.node("p1/b/r1").Status != NodeFailed || st.node("p2/b/r1").Status != NodeParked {
		t.Fatalf("merge %s, p1/b/r1 %s, p2/b/r1 %s", st.step("step:merge").Status, st.node("p1/b/r1").Status, st.node("p2/b/r1").Status)
	}
}

func TestPlanCompletes(t *testing.T) {
	l := newLab(t)
	e := l.add(`
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: p1, spec: tpl-plain.yaml}]
items: [a, b]
`)
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.fb.finish(l.fb.expOf("p1/b/r1"), PhaseDone, "")
	l.tick(e)
	if !l.state().Complete {
		t.Fatal("plan not complete")
	}
	l.tick(e) // a completed plan is left alone
}

func TestStatusShowsPerArmCountsAndNextRuns(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	s, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]ArmStatus{}
	for _, a := range s.Arms {
		by[a.Arm] = a
	}
	if by["p1"].Inflight != 2 || by["cold"].Done != 1 || by["cold"].Queued != 3 || by["p2"].Blocked != 4 || by["p1"].Queued != 2 {
		t.Fatalf("arms %+v", s.Arms)
	}
	if len(s.Next) == 0 || s.Next[0].Node != "cold/b/r1" {
		t.Fatalf("next %+v", s.Next)
	}
	var out strings.Builder
	s.Render(&out)
	if !strings.Contains(out.String(), "in-flight") || !strings.Contains(out.String(), "next:") {
		t.Fatalf("render:\n%s", out.String())
	}
}
