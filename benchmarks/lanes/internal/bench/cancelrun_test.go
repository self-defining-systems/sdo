//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// processAlive is whether pid is a live process (a zombie is dead: it only waits to be reaped).
func processAlive(pid int) bool {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(b)
	return !strings.HasPrefix(strings.TrimSpace(s[strings.LastIndex(s, ")")+1:]), "Z")
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil {
				return pid
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no pid in %s", path)
	return 0
}

func waitDead(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("%s (pid %d) is still running", what, pid)
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

// A cancelled adapter whose processes ignore TERM does not hold the step: the whole process group is killed once the
// grace passes, descendants included (Go's own WaitDelay kill reaches only the adapter's leader, so a child that
// ignored TERM lived on and kept the step's output pipe open).
func TestCancelledAdapterGroupIsKilledAfterTheGrace(t *testing.T) {
	old := adapterTermGrace
	adapterTermGrace = time.Second
	t.Cleanup(func() { adapterTermGrace = old })
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	w := adapter(t, "trap '' TERM\nsleep 600 &\necho $! > \"$CHILD_PID_FILE\"\nwait\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = w.Call(ctx, WorkloadCall{Verb: VerbRun, Action: "run", Env: []string{"CHILD_PID_FILE=" + pidFile}, Log: io.Discard})
	}()
	child := readPID(t, pidFile)
	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = syscall.Kill(child, syscall.SIGKILL)
		t.Fatal("the cancelled adapter was not ended after its grace")
	}
	waitDead(t, child, "the adapter's child")
}

// The manager's kill ends everything the step started, including the adapter, which runs in a process group of its
// own: a step killed alone leaves its run going on the lane.
func TestKillScriptEndsTheStepsAdapterGroupToo(t *testing.T) {
	dir := t.TempDir()
	stepPath := filepath.Join(dir, "step.json")
	pidFile := filepath.Join(dir, "adapter.pid")
	// A step: its own session (as `setsid -f` makes it) whose command line names step.json, with an adapter in a
	// process group of its own that ignores TERM.
	leader := exec.Command("setsid", "sh", "-c",
		"echo step-runner "+stepPath+" >/dev/null; (set -m; sh -c 'trap \"\" TERM; echo $$ > "+pidFile+"; sleep 600' &) ; sleep 600")
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = leader.Wait() }()
	adapterPID := readPID(t, pidFile)
	t.Cleanup(func() {
		_ = syscall.Kill(-leader.Process.Pid, syscall.SIGKILL)
		_ = syscall.Kill(adapterPID, syscall.SIGKILL)
	})
	start := time.Now()
	if err := exec.Command("sh", "-c", killScript(stepPath, 2)).Run(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("the kill took %s", took)
	}
	waitDead(t, leader.Process.Pid, "the step")
	waitDead(t, adapterPID, "the step's adapter")
}

// `lanes plan remove --cancel` reaches the experiment through the plan backend's Cancel; a run already on a lane is
// killed too (the plan's contract: cancel stops the runs in flight), not left to finish.
func TestPlanCancelKillsTheExperimentsRunStep(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	s := l.toRun("c1")
	b := &PlanBackend{Root: l.root}
	must(t, b.Cancel(context.Background(), "e1"))
	l.pass()
	killed := l.w.killedIDs()
	if len(killed) != 1 || killed[0] != s.ID {
		t.Fatalf("killed %v, want the run %s", killed, s.ID)
	}
}

// A plain `lanes remove` keeps its promise to let runs in flight finish.
func TestPlainRemoveLeavesTheRunStepRunning(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.add("e1", []string{"h1"}, "a@x", 0)
	l.toRun("c1")
	must(t, Remove(l.root, "e1", false))
	l.pass()
	if killed := l.w.killedIDs(); len(killed) != 0 {
		t.Fatalf("a plain remove killed %v", killed)
	}
}

// The workload is told the spec's bounds, so it can end a run that outlives them whatever it waits for.
func TestLaneEnvCarriesTheSpecsCapAndReadyTimeout(t *testing.T) {
	r := &stepRunner{s: Step{Lane: "c1", Cap: 1800, ReadyTimeout: 2400}}
	env := strings.Join(r.laneEnv(nil), "\n")
	for _, want := range []string{"BENCH_CAP_SECONDS=1800", "BENCH_READY_TIMEOUT_SECONDS=2400"} {
		if !strings.Contains(env, want) {
			t.Errorf("lane env lacks %s:\n%s", want, env)
		}
	}
}
