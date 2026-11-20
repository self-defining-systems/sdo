//go:build integration

package bench

import (
	"context"
	"strings"
	"testing"
)

// A step without an experiment (release, recover, unhold) takes the workload and the fuzzops checkout from what the
// lane was prepared with, never from "any pinned fuzzops on the host". Here another checkout sorts first on the host.
func TestReleaseAndRecoverUseTheWorkloadAndCheckoutOfTheLanesPrepared(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.moreFz = ", 0000000: {h1: /nonexistent/other-checkout}"
	l.specExtra = "workload: ./adapter-x\n"
	l.writeInventory()
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.runningStep("c1", StepBuild)
	l.buildOK("c1")
	l.pass()
	lane := l.m.lanes["c1"]
	if lane.Prepared == nil {
		t.Fatalf("lane has no prepared operator: %+v", lane)
	}
	want := testdata("fakefuzzops")
	for _, kind := range []string{StepRelease, StepRecover, StepUnhold} {
		lane.Step = nil
		if !l.m.start(context.Background(), lane, Step{Kind: kind, Prev: lane.Prepared}) {
			t.Fatalf("%s was refused", kind)
		}
		s := l.w.last("c1", kind)
		if s == nil || s.Fuzzops != want || s.Workload != "./adapter-x" || s.FuzzopsRev != "0b528ac" {
			t.Errorf("%s started with %+v, want checkout %s, workload ./adapter-x", kind, s, want)
		}
	}
}

// A lane whose prepared state records no checkout is refused with a reason; nothing is guessed.
func TestStepWithoutARecordedWorkloadIsRefused(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.pass()
	lane := l.m.lanes["c1"]
	lane.Step = nil
	var logs strings.Builder
	l.m.Log = &logs
	if l.m.start(context.Background(), lane, Step{Kind: StepRelease, Prev: &Prepared{Experiment: "e1"}}) {
		t.Fatal("a release with no recorded workload started")
	}
	if !strings.Contains(logs.String(), "no recorded workload") {
		t.Fatalf("no reason logged: %q", logs.String())
	}
}

// A lane prepared before the checkout was recorded takes it from the spec of the experiment that prepared it.
func TestLoadBackfillsThePreparedFromItsExperimentsSpec(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.specExtra = "workload: ./adapter-x\n"
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.pass()
	lf := l.lane("c1")
	lf.Step, lf.Prepared = nil, &Prepared{Experiment: "e1"}
	must(t, writeJSON(l.root.LanePath("c1"), lf))
	must(t, l.m.loadInventory())
	must(t, l.m.load())
	if p := l.m.lanes["c1"].Prepared; p.FuzzopsRev != "0b528ac" || p.Workload != "./adapter-x" {
		t.Fatalf("prepared %+v", p)
	}
}
