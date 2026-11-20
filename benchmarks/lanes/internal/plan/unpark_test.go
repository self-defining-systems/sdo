package plan

import (
	"strings"
	"testing"
)

// parkA parks item a: p1/a/r1 fails with oom, its rerun fails again, and cold/a/r1 fails the same way.
func parkA(l *lab, e *Engine) {
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseFailed, "oom")
	l.tick(e)
}

func TestRetryUnparksAnItemAndRequeuesItsNodesWithAFreshFailureCount(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	parkA(l, e)
	if st := l.state(); st.Parked["a"] != "oom" || st.node("p1/a/r1").Status != NodeFailed {
		t.Fatalf("setup: parked %v, node %+v", st.Parked, st.node("p1/a/r1"))
	}
	before := len(l.fb.subs)
	if err := RetryPlan(l.dir(), "a", false); err != nil {
		t.Fatal(err)
	}
	l.tick(e)
	st := l.state()
	if len(st.Parked) != 0 || st.Failures["a|oom"] != 0 {
		t.Fatalf("park not cleared: %v %v", st.Parked, st.Failures)
	}
	n := st.node("p1/a/r1")
	if n.Status != NodeSubmitted || len(l.fb.subs) <= before {
		t.Fatalf("not requeued and released: %+v", n)
	}
	// Experiment names and result rows stay unique: the attempt number only grows.
	if n.Attempt <= 2 {
		t.Fatalf("attempt %d reused an earlier experiment name", n.Attempt)
	}
	// The retry is logged and survives a restart; applying it twice does nothing.
	l.tick(l.open())
	if got := l.state().Retried["p1/a/r1"]; got.WasParked != "oom" || got.At.IsZero() {
		t.Fatalf("retry log %+v", got)
	}
	// One more oom is a first failure again: rerun, not parked.
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseFailed, "oom")
	l.tick(l.open())
	if st := l.state(); len(st.Parked) != 0 || st.node("p1/a/r1").Status != NodeSubmitted {
		t.Fatalf("fresh count lost: %v %+v", st.Parked, st.node("p1/a/r1"))
	}
}

func TestRetryRefusesNodesThatAreDoneOrInFlight(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	l.tick(e)
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.tick(e)
	if err := RetryPlan(l.dir(), "p1/a/r1", false); err == nil || !strings.Contains(err.Error(), "done") {
		t.Errorf("done node: %v", err)
	}
	if err := RetryPlan(l.dir(), "cold/a/r1", false); err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Errorf("in-flight node: %v", err)
	}
	if err := RetryPlan(l.dir(), "nope", false); err == nil {
		t.Error("unknown target accepted")
	}
	if err := RetryPlan(l.dir(), "b", false); err == nil {
		t.Error("item with nothing parked accepted")
	}
}

func TestRetryAllParkedAndStatusShowsWhenAndWhy(t *testing.T) {
	l := newLab(t)
	e := l.add(retryPlan)
	parkA(l, e)
	if err := RetryPlan(l.dir(), "", true); err != nil {
		t.Fatal(err)
	}
	l.tick(e)
	s, err := e.Status()
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	s.Render(&out)
	if !strings.Contains(out.String(), "retried at ") || !strings.Contains(out.String(), "(was parked: oom)") {
		t.Fatalf("status:\n%s", out.String())
	}
}
