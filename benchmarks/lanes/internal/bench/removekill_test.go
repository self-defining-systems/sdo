//go:build integration

package bench

import (
	"context"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Removing an experiment cancels its in-flight build, including one that waits for cores: the manager kills the
// step (the host's Kill escalates TERM to KILL) instead of leaving it until the lane-wait cap.
func TestRemoveKillsTheExperimentsInFlightBuild(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	s := l.runningStep("c1", StepBuild)
	must(t, Remove(l.root, "e1", true))
	l.pass()
	killed := l.w.killedIDs()
	if len(killed) != 1 || killed[0] != s.ID {
		t.Fatalf("killed %v, want the build %s", killed, s.ID)
	}
}

// A build waiting in lane-wait ends on TERM (what Kill sends first) instead of waiting out the core cap.
func TestLaneWaitEndsOnTerm(t *testing.T) {
	r := limitRunner(t, 3)
	r.s.ClusterUp = "touch ran"
	other := CPUAllocator{Dir: r.s.Limits.State, Allowed: []int{2, 3, 4, 5, 6, 7}, Topo: topo(8, 1)}
	if _, err := other.Take("other", 6, 0); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(100 * time.Millisecond); _ = syscall.Kill(os.Getpid(), syscall.SIGTERM) }()
	done := make(chan bool, 1)
	go func() { done <- r.makeClusterUp(context.Background()) }()
	select {
	case ok := <-done:
		if ok || !strings.Contains(r.res.Reason, "cancelled") {
			t.Fatalf("ok %v reason %q", ok, r.res.Reason)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lane-wait ignored TERM")
	}
}
