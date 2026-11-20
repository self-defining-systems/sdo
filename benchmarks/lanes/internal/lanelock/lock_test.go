//go:build integration

package lanelock

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/env"
)

// One owner per lane: a second taker fails at once, another lane is free, and
// the lock is free again after release.
func TestTakeIsExclusivePerLane(t *testing.T) {
	dir := t.TempDir()
	a := env.Env{KindCluster: "c1", Namespace: "ns", StateDir: dir}
	release, err := Take(a, "run")
	if err != nil {
		t.Fatal(err)
	}
	if !Held(a) {
		t.Fatal("not held while taken")
	}
	if _, err := Take(a, "recover"); !errors.Is(err, ErrBusy) {
		t.Fatalf("second take: %v", err)
	}
	other, err := Take(env.Env{KindCluster: "c2", Namespace: "ns", StateDir: dir}, "run")
	if err != nil {
		t.Fatalf("another cluster's lane: %v", err)
	}
	other()
	release()
	if Held(a) {
		t.Fatal("held after release")
	}
	again, err := Take(a, "run")
	if err != nil {
		t.Fatal(err)
	}
	again()
}

// The holder is this test binary re-run as a helper: one process that takes
// the lane, says so on stdout and waits to be killed. (flock(1) is not used:
// it forks the command, so killing it can leave a child holding the lock, and
// polling for the lock is a race against a loaded machine.)
const holderEnv = "LANELOCK_TEST_HOLDER_DIR"

var heldForever func()

func TestMain(m *testing.M) {
	if dir := os.Getenv(holderEnv); dir != "" {
		if _, err := Take(env.Env{KindCluster: "c", Namespace: "ns", StateDir: dir}, "holder"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("held")
		select {}
	}
	os.Exit(m.Run())
}

// A holder that dies frees the lane (flock is released with the process).
func TestLockOfAKilledHolderIsFree(t *testing.T) {
	dir := t.TempDir()
	e := env.Env{KindCluster: "c", Namespace: "ns", StateDir: dir}
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), holderEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "held\n" {
		t.Fatalf("holder did not report holding the lock: %q, %v", line, err)
	}
	if !Held(e) {
		t.Fatal("lane not held while the holder lives")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait() // returns once the process has exited, so the kernel has dropped its locks
	release, err := Take(e, "run")
	if err != nil {
		t.Fatalf("after the holder died: %v", err)
	}
	release()
}
