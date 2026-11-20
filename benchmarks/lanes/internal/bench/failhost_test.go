//go:build integration

package bench

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A host whose sshd never answers (a wedged kernel) must not hold the manager: the pass ends within the probe bound,
// the other host's work goes on, and nothing starts on the wedged one.
func TestHostThatHangsNeverBlocksTheManagerLoop(t *testing.T) {
	tune(t, &probeBound, 100*time.Millisecond)
	tune(t, &hostCallBound, 100*time.Millisecond)
	l := newFlab(t, "h1=c1", "h2=c2")
	l.add("e1", []string{"h1", "h2"}, "a@x", 0)
	l.w.set("h1", hostHangs)
	l.pass()
	hs := l.hostStatus()["h1"]
	if hs.Error == "" || hs.DownSince.IsZero() {
		t.Fatalf("h1 is not marked unreachable with a reason and a time: %+v", hs)
	}
	if hs2 := l.hostStatus()["h2"]; hs2.Error != "" {
		t.Fatalf("h2 marked unreachable: %+v", hs2)
	}
	if l.w.last("c1", "") != nil {
		t.Fatal("a step was started on the unreachable host")
	}
	if s := l.w.last("c2", StepBuild); s == nil || s.Experiment != "e1" {
		t.Fatalf("the healthy host did not take the work: %+v", s)
	}
}

// A call that hangs after the probe passed (the host wedged between the two) is bounded as well.
func TestHostCallThatHangsIsBounded(t *testing.T) {
	tune(t, &probeBound, 100*time.Millisecond)
	tune(t, &hostCallBound, 100*time.Millisecond)
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.w.set("h1", hostHangs+":Start")
	l.pass() // the build's Start hangs; the pass must still end
	if l.lane("c1").Step == nil {
		t.Fatal("the lane lost the step it asked for (the probe decides whether it runs)")
	}
}

// In-flight steps on a host that stays unreachable are failed as "host unreachable", cost the item no retry, go back to
// the queue for another host, and an alarm fires. The host is admitted again only after its preflight passes.
func TestHostUnreachableFailsStepsWithoutCostingARetryAndRequeues(t *testing.T) {
	tune(t, &probeBound, 100*time.Millisecond)
	l := newFlab(t, "h1=c1", "h2=c2")
	l.add("e1", []string{"h1", "h2"}, "a@x", 0) // no retry budget at all
	run := l.toRun("c1")
	l.w.set("h1", hostRefuses)
	l.pass()
	if l.lane("c1").Step == nil || len(l.exp("e1").Runs) != 0 {
		t.Fatalf("a blip below the grace period lost the step: %+v", l.lane("c1"))
	}
	if hs := l.hostStatus()["h1"]; hs.Error == "" || !strings.Contains(hs.Error, "MaxStartups") || hs.DownSince.IsZero() {
		t.Fatalf("h1 status %+v", hs)
	}
	n := l.w.nStarted()
	l.advance(hostGrace + time.Second)

	lf := l.lane("c1")
	if lf.Step != nil || !strings.Contains(lf.Reason, "host unreachable") {
		t.Fatalf("the step on the unreachable host is not failed with its reason: %+v", lf)
	}
	x := l.exp("e1")
	if len(x.Runs) != 0 {
		t.Fatalf("host loss was recorded as a run failure (it would use the item's retry): %+v", x.Runs)
	}
	if p := x.Progress(Seq{"a@x", 1}); p.State != SeqQueued || p.Attempt != 1 {
		t.Fatalf("the item is %s attempt %d: want queued on attempt 1", p.State, p.Attempt)
	}
	if !l.hasAlarm("host-unreachable", "h1") {
		t.Fatalf("no host-unreachable alarm: %v", l.alarms())
	}
	if l.w.startsOn("h1", n) != 0 {
		t.Fatal("a step was started on the unreachable host")
	}
	// The item goes to the other host.
	if s := l.runningStep("c2", StepBuild); s.Experiment != "e1" || s.Host != "h2" {
		t.Fatalf("not requeued on h2: %+v", s)
	}
	_ = run

	// The host comes back. Its old step is still running there: nothing new starts on that lane until it is over, and
	// not before the host's preflight passes.
	preflight := fmt.Errorf("docker is down")
	l.m.HostPreflight = func(context.Context, string) error { return preflight }
	l.w.set("h1", hostUp)
	l.pass()
	if l.w.startsOn("h1", n) != 0 {
		t.Fatalf("a step started on h1 before its preflight passed: %+v", l.w.last("c1", ""))
	}
	if hs := l.hostStatus()["h1"]; !strings.Contains(hs.Error, "preflight") {
		t.Fatalf("status does not say why h1 is not admitted: %+v", hs)
	}
	preflight = nil
	l.pass()
	l.w.finish(run.ID, Result{OK: false, Reason: "late"}) // the orphan ends; its result is discarded
	l.pass()
	l.pass()
	if rec := l.w.last("c1", StepRecover); rec == nil {
		t.Fatalf("lane c1 was not recovered before reuse: %+v", l.lane("c1"))
	}
	if len(l.exp("e1").Runs) != 0 && l.exp("e1").Runs[RunID("a@x", 1, 1, 1)].Reason == "late" {
		t.Fatal("the orphaned step's late result was adopted")
	}
}

// With no other host the item waits for this one, and runs when it is back; it is never invalid.
func TestHostUnreachableWithNoOtherHostLeavesTheItemWaiting(t *testing.T) {
	tune(t, &probeBound, 100*time.Millisecond)
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.toRun("c1")
	l.w.set("h1", hostRefuses)
	l.pass()
	l.advance(hostGrace + time.Second)
	l.advance(time.Hour)
	if p := l.exp("e1").Progress(Seq{"a@x", 1}); p.State != SeqQueued {
		t.Fatalf("item %s (%s): want queued", p.State, p.Reason)
	}
	l.w.set("h1", hostUp)
	for i := 0; i < 8 && l.w.last("c1", StepRecover) == nil; i++ {
		l.pass()
	}
	rec := l.w.last("c1", StepRecover)
	if rec == nil {
		t.Fatalf("no recover step after the host returned: %+v", l.lane("c1"))
	}
	l.w.finish(rec.ID, Result{OK: true})
	l.runningStep("c1", StepBuild)
}

// A step that is still running long after any bound is killed and the reason is recorded on its run; the item retries.
func TestStepThatHangsPastItsBoundIsKilledWithAReason(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 1)
	run := l.toRun("c1")
	l.advance(time.Minute)
	if len(l.w.killedIDs()) != 0 {
		t.Fatal("a step was killed inside its bound")
	}
	l.advance(6 * time.Hour)
	l.pass()
	if k := l.w.killedIDs(); len(k) != 1 || k[0] != run.ID {
		t.Fatalf("killed %v, want [%s]", k, run.ID)
	}
	r := l.exp("e1").Runs[RunID("a@x", 1, 1, 1)]
	if r.Status != StatusInvalid || !strings.Contains(r.Reason, "exceeded its bound") {
		t.Fatalf("the reason is not recorded on the run: %+v", r)
	}
	if !strings.Contains(readString(l.root.path("bench.log")), "exceeded its bound") {
		t.Fatal("bench.log does not say why the step was killed")
	}
}

func TestDiskFloorAdmitsNoBuildAndRaisesAnAlarm(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = ", min_free: 10G"
	l.writeInventory()
	l.w.free["h1"] = 1 << 30
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.pass()
	l.pass()
	if l.w.last("c1", "") != nil {
		t.Fatalf("a build was admitted below the disk floor: %+v", l.w.last("c1", ""))
	}
	if !l.hasAlarm("disk-floor", "h1") {
		t.Fatalf("no disk-floor alarm: %v", l.alarms())
	}
	n := len(l.alarms())
	l.pass()
	if len(l.alarms()) != n {
		t.Fatalf("the alarm repeats while the condition stands: %v", l.alarms())
	}
	l.w.free["h1"] = 50 << 30
	l.runningStep("c1", StepBuild)
}
