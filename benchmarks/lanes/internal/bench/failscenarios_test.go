//go:build integration

package bench

import (
	"os"
	"strings"
	"testing"
	"time"
)

// tune sets a package tunable for one test.
func tune[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

// ---- 2. Manager restart ----

// A restarted manager adopts a step that is still running (no second step), adopts its result when it ends, and a
// step that died while the manager was away is lost and retried once, never duplicated.
func TestManagerRestartAdoptsRunningStepsAndRetriesLostOnesOnce(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 1)
	run := l.toRun("c1")
	n := l.w.nStarted()

	restart := func() {
		l.m = &Manager{Root: l.root, Poll: time.Second, Ops: l.w.ops, Log: os.Stderr}
		l.m.Now = func() time.Time { return l.now }
	}
	restart()
	l.pass()
	l.pass()
	if l.w.nStarted() != n || l.lane("c1").Step == nil || l.lane("c1").Step.ID != run.ID {
		t.Fatalf("restart duplicated or dropped the running step: started %d->%d, lane %+v", n, l.w.nStarted(), l.lane("c1"))
	}
	l.w.finish(run.ID, Result{OK: true, Status: StatusDone, Prepared: &Prepared{Experiment: "e1", Namespace: "app1", WorkloadRev: "0b528ac"}})
	restart()
	l.pass()
	if p := l.exp("e1").Progress(Seq{"a@x", 1}); p.State != SeqDone {
		t.Fatalf("the finished step was not adopted after restart: %+v", p)
	}
	if got := len(l.exp("e1").Runs); got != 1 {
		t.Fatalf("%d runs recorded for one run step", got)
	}

	// A second item whose step dies while the manager is away.
	l2 := newFlab(t, "h1=c1")
	l2.add("e2", []string{"h1"}, "a@x", 1)
	run2 := l2.toRun("c1")
	l2.w.mu.Lock()
	l2.w.steps[run2.ID].state = StepDead
	l2.w.mu.Unlock()
	l2.m = &Manager{Root: l2.root, Poll: time.Second, Ops: l2.w.ops, Log: os.Stderr}
	l2.m.Now = func() time.Time { return l2.now }
	l2.pass()
	l2.pass()
	runs := l2.exp("e2").Runs
	if r := runs[RunID("a@x", 1, 1, 1)]; r.Status != StatusInvalid || !strings.Contains(r.Reason, "lane lost") {
		t.Fatalf("lost step not marked lost: %+v", runs)
	}
	recovers := 0
	for _, s := range l2.w.started {
		if s.Kind == StepRecover {
			recovers++
		}
	}
	if recovers != 1 {
		t.Fatalf("%d recover steps for one lost step", recovers)
	}
	// Retried once: the next attempt's run is the only new run step, and a further restart starts none.
	l2.w.finish(l2.w.last("c1", StepRecover).ID, Result{OK: true})
	for i := 0; i < 4; i++ {
		l2.pass()
	}
	l2.m = &Manager{Root: l2.root, Poll: time.Second, Ops: l2.w.ops, Log: os.Stderr}
	l2.m.Now = func() time.Time { return l2.now }
	for i := 0; i < 3; i++ {
		l2.pass()
	}
	builds := 0
	for _, s := range l2.w.started {
		if s.Kind == StepBuild {
			builds++
		}
	}
	if builds > 2 {
		t.Fatalf("%d builds for an item with one retry", builds)
	}
}

// ---- 3. A lane goes down ----

// The lane's cluster is gone: the step fails with a clear reason, the lane is reset by policy and then retired, and the
// item retries on another lane.
func TestLaneDownMidStepRetriesTheItemOnAnotherLane(t *testing.T) {
	tune(t, &downRetryAfter, 5*time.Minute)
	l := newFlab(t, "h1=c1,c2")
	l.extra["h1"] = ", max_builds: 2"
	l.writeInventory()
	l.add("e1", []string{"h1"}, "a@x", 1)
	run := l.toRun("c1")
	l.w.finish(run.ID, Result{OK: false, Status: StatusInvalid, Reason: "cluster c1 is gone: no nodes", LaneDown: "cluster c1 is gone"})
	l.pass()
	lf := l.lane("c1")
	if lf.State != LaneDown || !strings.Contains(lf.Reason, "gone") {
		t.Fatalf("lane c1: %+v", lf)
	}
	r := l.exp("e1").Runs[RunID("a@x", 1, 1, 1)]
	if r.Status != StatusInvalid || !strings.Contains(r.Reason, "cluster c1 is gone") {
		t.Fatalf("the failed run has no clear reason: %+v", r)
	}
	if s := l.runningStep("c2", StepBuild); s.Experiment != "e1" {
		t.Fatalf("retry not on the other lane: %+v", s)
	}
	// By policy: reset after the wait, retired after maxAutoResets.
	l.advance(10 * time.Minute)
	if got := l.lane("c1"); got.State != LaneFree || got.AutoResets != 1 {
		t.Fatalf("c1 not reset by policy: %+v", got)
	}
}

// ---- 9. A reservation lost ----

// The host rebooted and its lane block is gone: admission refuses lanes outside the reserved block, and status says so.
func TestLostReservationStopsAdmissionAndStatusSaysSo(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = ", reserve: {cpus: \"8-15\", memory: 32Gi}"
	l.writeInventory()
	f := &fakeHost{block: "8-15", other: "0-7,16-31"}
	l.m.Helper = f.helper
	l.m.DriftEvery = time.Nanosecond
	must(t, writeJSON(l.root.reservationPath("h1"), Reservation{Host: "h1", Block: "8-15", TwoWay: true}))
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.pass()
	if l.w.last("c1", StepBuild) == nil {
		t.Fatalf("the reserved host did not admit a build: %v", l.m.drift)
	}
	// Reboot: the slices are back to their defaults and the block is gone.
	f.block, f.other = "", ""
	l.w.mu.Lock()
	l.w.started = nil
	l.w.mu.Unlock()
	l.m.lanes = nil
	l.now = l.now.Add(time.Minute)
	time.Sleep(time.Millisecond)
	l.add("e2", []string{"h1"}, "b@y", 0)
	l.pass()
	if why := l.m.drift["h1"]; why == "" {
		t.Fatalf("no drift recorded for a lost reservation")
	}
	for _, s := range l.w.started {
		if s.Experiment == "e2" {
			t.Fatalf("a lane was placed on a host whose reservation is lost: %+v", s)
		}
	}
	var out strings.Builder
	must(t, Status(l.root, "", &out))
	if !strings.Contains(out.String(), "h1") || !strings.Contains(strings.ToLower(out.String()), "reservation") {
		t.Fatalf("status does not say the reservation is lost:\n%s", out.String())
	}
}
