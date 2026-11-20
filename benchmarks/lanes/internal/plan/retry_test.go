package plan

import (
	"strings"
	"testing"
)

const retryPlan = `
plan: tp
pins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}
arms: [{name: p1, spec: tpl-plain.yaml}, {name: cold, spec: tpl-plain.yaml}]
items: [a, b]
parallel: 8
`

func TestFirstFailureRerunsUnderANewSuffixAndOnlyTheRunnerRetries(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	first := l.fb.expOf("p1/a/r1")
	l.fb.finish(first, PhaseFailed, "oom")
	l.tick(e)
	second := l.fb.expOf("p1/a/r1")
	if second != first+"-x1" {
		t.Fatalf("rerun %q, want %q", second, first+"-x1")
	}
	n := l.state().node("p1/a/r1")
	if n.Status != NodeSubmitted || n.Attempt != 2 || len(n.Attempts) != 1 || n.Attempts[0].Reason != "oom" {
		t.Fatalf("node %+v", n)
	}
	// The spec of the rerun forbids in-cell retries, whatever the template said.
	if !strings.Contains(string(mustRead(t, l.fb.subs[len(l.fb.subs)-1].SpecPath)), `"retries": 0`) {
		t.Fatal("rerun spec allows retries")
	}
}

func TestSecondIdenticalFailureParksTheItemOnAnyArm(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	// Item a: p1 fails with oom, then cold of the same item fails with oom too: two identical failures of the item.
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.tick(e)
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseFailed, "oom")
	l.tick(e)
	st := l.state()
	if st.Parked["a"] != "oom" {
		t.Fatalf("parked %v", st.Parked)
	}
	if st.node("cold/a/r1").Status != NodeFailed {
		t.Fatalf("cold/a/r1 = %s", st.node("cold/a/r1").Status)
	}
	// p1/a/r1 was rerun (submitted) when the item got parked: it is left to finish, not killed, and not rerun again.
	if st.node("p1/a/r1").Status != NodeSubmitted {
		t.Fatalf("p1/a/r1 = %s", st.node("p1/a/r1").Status)
	}
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.tick(e)
	if got := l.state().node("p1/a/r1"); got.Status != NodeFailed {
		t.Fatalf("a parked item's failure is final: %s", got.Status)
	}
	// Item b is untouched.
	if got := l.state().node("p1/b/r1").Status; got != NodeSubmitted {
		t.Fatalf("p1/b/r1 = %s", got)
	}
}

func TestDifferentReasonsDoNotPark(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "capped")
	l.tick(e)
	st := l.state()
	if len(st.Parked) != 0 || st.node("p1/a/r1").Status != NodeSubmitted || st.node("p1/a/r1").Attempt != 3 {
		t.Fatalf("parked %v, node %+v", st.Parked, st.node("p1/a/r1"))
	}
}

func TestParkingSurvivesARestartAndLostExperimentsCount(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "claim-wait")
	l.tick(e)
	e = l.open()
	delete(l.fb.exps, l.fb.expOf("p1/a/r1")) // the experiment vanished: reason "lost"
	l.fb.finish(l.fb.expOf("p1/b/r1"), PhaseFailed, "claim-wait")
	l.tick(e)
	if st := l.state(); st.Failures["a|claim-wait"] != 1 || st.Failures["b|claim-wait"] != 1 || st.node("p1/a/r1").Attempts[len(st.node("p1/a/r1").Attempts)-1].Reason != "lost" {
		t.Fatalf("failures %v attempts %+v", st.Failures, st.node("p1/a/r1").Attempts)
	}
	l.tick(l.open())
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "claim-wait")
	l.tick(l.open())
	if st := l.state(); st.Parked["a"] != "claim-wait" {
		t.Fatalf("a restart forgot the failure count: %v", st.Failures)
	}
}
