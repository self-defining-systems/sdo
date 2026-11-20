package plan

import (
	"path/filepath"
	"reflect"
	"testing"
)

func (l *lab) dir() string { return filepath.Join(l.root, "plans", "tp") }

func TestPausedPlanReleasesNothingAndResumeContinues(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	released := len(l.fb.nodes())
	if released != 2 {
		t.Fatalf("released %d", released)
	}
	if err := PausePlan(l.dir()); err != nil {
		t.Fatal(err)
	}
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(e)
	if got := len(l.fb.nodes()); got != released {
		t.Fatalf("a paused plan released %v", l.fb.nodes())
	}
	if n := l.state().node("p1/a/r1"); n.Status != NodeDone {
		t.Fatalf("a paused plan must still record what ended: %s", n.Status)
	}
	// A restarted manager (a fresh engine over the same directory) is still paused.
	l.tick(l.open())
	if got := len(l.fb.nodes()); got != released {
		t.Fatalf("pause lost across a restart: %v", l.fb.nodes())
	}
	if err := ResumePlan(l.dir()); err != nil {
		t.Fatal(err)
	}
	l.tick(l.open())
	if got := len(l.fb.nodes()); got <= released {
		t.Fatalf("resume released nothing: %v", l.fb.nodes())
	}
}

func TestSkippedItemLetsInflightFinishAndSkipsTheRest(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e) // p1/a/r1 and cold/a/r1 in flight
	if err := SkipItem(l.dir(), "a", false); err != nil {
		t.Fatal(err)
	}
	if err := SkipItem(l.dir(), "nope", false); err == nil {
		t.Error("skip of an unknown item accepted")
	}
	l.tick(e)
	st := l.state()
	if st.node("p2/a/r1").Status != NodeSkipped || st.node("p2/a/r2").Status != NodeSkipped {
		t.Fatalf("unreleased nodes of the item: %s %s", st.node("p2/a/r1").Status, st.node("p2/a/r2").Status)
	}
	if st.node("p1/a/r1").Status != NodeSubmitted || len(l.fb.cancelled) != 0 {
		t.Fatalf("in-flight must finish: %s, cancelled %v", st.node("p1/a/r1").Status, l.fb.cancelled)
	}
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.tick(e)
	l.tick(e)
	for _, n := range l.fb.nodes() {
		if n == "p2/a/r1" {
			t.Fatalf("a skipped item's dependent was released: %v", l.fb.nodes())
		}
	}
	if got := l.fb.nodes(); !reflect.DeepEqual(got[len(got)-1:], []string{"p1/b/r1"}) && l.state().node("p1/b/r1").Status == NodeWaiting {
		t.Errorf("other items must go on: %v", got)
	}
}

func TestSkipWithCancelCancelsTheItemsInflightRuns(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	exp := l.fb.expOf("p1/a/r1")
	if err := SkipItem(l.dir(), "a", true); err != nil {
		t.Fatal(err)
	}
	l.tick(e)
	l.tick(e) // idempotent
	if !reflect.DeepEqual(l.fb.cancelled[:1], []string{exp}) {
		t.Fatalf("cancelled %v want %s first", l.fb.cancelled, exp)
	}
	n := l.state().node("p1/a/r1")
	if n.Status != NodeSkipped || n.Reason == "" {
		t.Fatalf("a cancelled node is skipped with a reason: %s %q", n.Status, n.Reason)
	}
	if got := l.state().Parked; len(got) != 0 {
		t.Errorf("a skip must not park or count as a failure: %v", got)
	}
}

func TestRemovedPlanDrainsAndCompletes(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	if err := RemovePlan(l.dir(), false); err != nil {
		t.Fatal(err)
	}
	l.tick(l.open())
	st := l.state()
	for _, n := range st.Nodes {
		if n.Status == NodeWaiting {
			t.Fatalf("%s still waiting after remove", n.ID)
		}
	}
	if st.Complete {
		t.Fatal("complete while two runs are in flight")
	}
	l.fb.finish(l.fb.expOf("p1/a/r1"), PhaseDone, "")
	l.fb.finish(l.fb.expOf("cold/a/r1"), PhaseDone, "")
	l.tick(l.open())
	l.tick(l.open())
	if !l.state().Complete {
		t.Fatalf("plan not complete after the in-flight runs ended")
	}
	if len(l.fb.nodes()) != 2 {
		t.Fatalf("a removed plan released more: %v", l.fb.nodes())
	}
	rows, _, _ := Rows(l.dir())
	if len(rows) != 2 {
		t.Errorf("results of what ran must stay: %d rows", len(rows))
	}
}

func TestRemoveWithCancelCancelsAllInflight(t *testing.T) {
	l := newLab(t)
	e := l.add(twopass)
	l.tick(e)
	if err := RemovePlan(l.dir(), true); err != nil {
		t.Fatal(err)
	}
	l.tick(e)
	if len(l.fb.cancelled) != 2 {
		t.Fatalf("cancelled %v", l.fb.cancelled)
	}
	l.tick(e)
	if !l.state().Complete {
		t.Fatal("a removed plan with nothing in flight must be complete")
	}
}
