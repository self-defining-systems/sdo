//go:build integration

package bench

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// Lanes that left hosts.yaml (the ghost lanes of LB17): a lane outside the inventory is closed whatever its state file
// says. A dead step (killed, host rebooted) retires it; a live step drains it; its cluster is deleted and its cores
// given back; status counts the retirements.

func ghostLane(l *flab, lane string, step *StepRef, prepared *Prepared) {
	must(l.t, writeJSON(l.root.LanePath(lane), LaneFile{Lane: lane, Host: "h1", State: LaneBuilding, Since: t0, Step: step, Prepared: prepared}))
}

func retired(l *flab, lane string) bool {
	return !fileExists(l.root.LanePath(lane)) && fileExists(l.root.path("lanes", "retired", lane+".json"))
}

// The ghost's step process is dead: the lane is retired (state moved, logged, counted), not kept "building" forever.
func TestLeftLaneWithADeadStepIsRetired(t *testing.T) {
	l := newFlab(t, "h1=c1")
	ghostLane(l, "g1", &StepRef{ID: "build-dead", Kind: StepBuild, Started: t0}, nil)
	l.pass()
	if !retired(l, "g1") {
		t.Fatalf("g1 not retired; bench.log:\n%s", readString(l.root.path("bench.log")))
	}
	if !strings.Contains(readString(l.root.path("bench.log")), "lane g1: building -> retired") {
		t.Fatalf("not logged:\n%s", readString(l.root.path("bench.log")))
	}
	if n := l.w.nStarted(); n != 0 {
		t.Fatalf("%d steps started for a lane that left (a recover on a ghost)", n)
	}
	var out bytes.Buffer
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "1 lanes retired") || strings.Contains(out.String(), "g1") {
		t.Fatalf("status:\n%s", out.String())
	}
}

// A ghost whose step is still alive drains: no new step, and it is retired once the step ends.
func TestLeftLaneWithALiveStepDrainsThenRetires(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.w.mu.Lock()
	l.w.steps["build-live"] = &fakeStep{s: Step{ID: "build-live", Kind: StepBuild, Lane: "g1"}, host: "h1", state: StepRunning}
	l.w.mu.Unlock()
	ghostLane(l, "g1", &StepRef{ID: "build-live", Kind: StepBuild, Started: t0}, nil)
	l.pass()
	l.advance(time.Minute)
	if retired(l, "g1") {
		t.Fatal("retired while its step still runs")
	}
	if n := l.w.nStarted(); n != 0 {
		t.Fatalf("%d new steps for a draining lane", n)
	}
	var lf LaneFile
	must(t, readJSON(l.root.LanePath("g1"), &lf))
	if !strings.Contains(lf.Reason, "draining") || lf.Step == nil {
		t.Fatalf("lane file %+v", lf)
	}
	l.w.mu.Lock()
	l.w.steps["build-live"].state = StepDead
	l.w.mu.Unlock()
	l.advance(time.Minute)
	if !retired(l, "g1") {
		t.Fatal("not retired after its step ended")
	}
}

// A ghost that was built has its cluster deleted (a release step, which gives the lane's cores back) before it is
// retired.
func TestLeftLaneHasItsClusterDeletedBeforeItIsRetired(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = `, cluster_up: "true {evidence}", cluster_down: "true {cluster}"`
	l.writeInventory()
	prep := &Prepared{Experiment: "x", Namespace: "app1", AppPack: "/p", Workload: "fuzzops", WorkloadRev: "0b528ac"}
	ghostLane(l, "g1", &StepRef{ID: "build-dead", Kind: StepBuild, Started: t0}, prep)
	l.pass()
	rel := l.w.last("g1", StepRelease)
	if rel == nil || !rel.DeleteCluster {
		t.Fatalf("no cluster-deleting release step for the ghost; started %d", l.w.nStarted())
	}
	if retired(l, "g1") {
		t.Fatal("retired before its cluster was deleted")
	}
	l.w.finish(rel.ID, Result{OK: true, Gone: true})
	l.advance(time.Minute)
	l.advance(time.Minute)
	if !retired(l, "g1") {
		t.Fatalf("not retired after the release; lane file exists: %v", fileExists(l.root.LanePath("g1")))
	}
}
