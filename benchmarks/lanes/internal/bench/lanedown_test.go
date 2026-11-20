//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type gcSpy struct {
	HostOps
	keep []string
}

func (g *gcSpy) GC(_ context.Context, keep []string) ([]string, error) {
	g.keep = keep
	return nil, nil
}

// gc keeps the step.lock of every lane that has a step, whether or not the lane's cluster exists yet (a lane's first
// build runs while the lane is Gone): collecting the lock of a live step kills it.
func TestGCKeepsTheStepLockOfEveryLaneWithAStep(t *testing.T) {
	spy := &gcSpy{}
	m := &Manager{Root: Root(t.TempDir()), inv: Inventory{Hosts: map[string]Host{"h1": {}}},
		exps: map[string]*Experiment{},
		lanes: map[string]*LaneFile{
			"new":  {Lane: "new", Host: "h1", Gone: true, State: LaneBuilding, Step: &StepRef{ID: "s1"}},
			"idle": {Lane: "idle", Host: "h1", Gone: true, State: LaneFree},
		},
		Ops: func(string, Host) HostOps { return spy }}
	m.gcHost(context.Background(), "h1")
	keep := strings.Join(spy.keep, ",")
	if !strings.Contains(keep, filepath.Join("new", "step.lock")) {
		t.Fatalf("a live step's lock is collectable; keep = %v", spy.keep)
	}
	if strings.Contains(keep, filepath.Join("idle", "step.lock")) {
		t.Fatalf("a gone lane with no step keeps a lock; keep = %v", spy.keep)
	}
}

// A recover step on a lane whose cluster is gone has no fault to undo: it succeeds and the lane starts from nothing.
func TestRecoverOfAMissingClusterIsNoFault(t *testing.T) {
	r := newRunner(t, missingCluster)
	r.s.ClusterUp = "true"
	prev := &Prepared{Experiment: "x", Namespace: "ns", AppPack: "/nonexistent"}
	r.s.Prev, r.res.Prepared = prev, prev
	r.recover(context.Background())
	if !r.res.OK || r.res.LaneDown != "" || !r.res.Gone || r.res.Prepared != nil {
		t.Fatalf("ok=%v down=%q gone=%v prepared=%v", r.res.OK, r.res.LaneDown, r.res.Gone, r.res.Prepared)
	}
}

func downLane(t *testing.T, l *lab, resets int) {
	t.Helper()
	lf := LaneFile{Lane: "c1", Host: "h1", State: LaneDown, Reason: "3 builds failed in a row: x", Since: time.Now().Add(-time.Hour), AutoResets: resets, BuildFailures: 3}
	must(t, writeJSON(l.root.LanePath("c1"), lf))
}

// A lane down for a lane-specific cause is recreated by the manager after a bounded number of attempts, then retired
// with the reason logged; status shows it. No human `lane reset` is needed.
func TestDownLaneIsAutoResetThenRetired(t *testing.T) {
	old := downRetryAfter
	downRetryAfter = 0
	t.Cleanup(func() { downRetryAfter = old })
	l := newLab(t)
	downLane(t, l, 0)
	must(t, l.m.Reconcile(context.Background()))
	lf := l.lane()
	if lf.State != LaneFree || lf.AutoResets != 1 || lf.BuildFailures != 0 {
		t.Fatalf("not reset: %+v", lf)
	}
	if !strings.Contains(l.readRoot("bench.log"), "auto-reset 1/") {
		t.Fatalf("bench.log:\n%s", l.readRoot("bench.log"))
	}
	downLane(t, l, maxAutoResets)
	must(t, l.m.Reconcile(context.Background()))
	lf = l.lane()
	if lf.State != LaneRetired || !strings.Contains(lf.Reason, "3 builds failed in a row") {
		t.Fatalf("not retired with its reason: %+v", lf)
	}
	if !strings.Contains(l.readRoot("bench.log"), "down -> retired") {
		t.Fatalf("bench.log:\n%s", l.readRoot("bench.log"))
	}
	must(t, l.m.Reconcile(context.Background()))
	if got := l.lane().State; got != LaneRetired {
		t.Fatalf("a retired lane came back: %s", got)
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "retired") || !strings.Contains(out.String(), "3 builds failed in a row") {
		t.Fatalf("status:\n%s", out.String())
	}
}

// A lane down for less than downRetryAfter is left alone.
func TestDownLaneWaitsBeforeAutoReset(t *testing.T) {
	l := newLab(t)
	lf := LaneFile{Lane: "c1", Host: "h1", State: LaneDown, Reason: "x", Since: time.Now()}
	must(t, writeJSON(l.root.LanePath("c1"), lf))
	must(t, l.m.Reconcile(context.Background()))
	if got := l.lane().State; got != LaneDown {
		t.Fatalf("state %s", got)
	}
}

// A lane that left hosts.yaml closes its state: bench.log says it was retired (so lane-time totals stop counting it)
// and its files move to lanes/retired.
func TestLaneLeavingTheInventoryIsRetired(t *testing.T) {
	l := newLab(t)
	must(t, l.m.Reconcile(context.Background()))
	gone := LaneFile{Lane: "old1", Host: "h9", State: LaneBuilding, Since: time.Now().Add(-time.Hour)}
	must(t, writeJSON(l.root.LanePath("old1"), gone))
	must(t, os.WriteFile(l.root.TimelinePath("old1"), []byte("{}\n"), 0o644))
	must(t, l.m.Reconcile(context.Background()))
	if !strings.Contains(l.readRoot("bench.log"), "lane old1: building -> retired") {
		t.Fatalf("bench.log:\n%s", l.readRoot("bench.log"))
	}
	if fileExists(l.root.LanePath("old1")) || !fileExists(l.root.path("lanes", "retired", "old1.json")) || !fileExists(l.root.path("lanes", "retired", "old1.timeline.jsonl")) {
		t.Fatal("lane files not moved to lanes/retired")
	}
	lanes, _ := l.root.Lanes()
	for _, x := range lanes {
		if x.Lane == "old1" {
			t.Fatal("status still lists the retired lane")
		}
	}
	n := strings.Count(l.readRoot("bench.log"), "old1: building -> retired")
	must(t, l.m.Reconcile(context.Background()))
	if strings.Count(l.readRoot("bench.log"), "old1: building -> retired") != n {
		t.Fatal("retirement logged twice")
	}
}
