//go:build integration

package lanelock

import (
	"errors"
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

// A holder that dies frees the lane (flock is released with the process).
func TestLockOfAKilledHolderIsFree(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("no flock(1)")
	}
	dir := t.TempDir()
	e := env.Env{KindCluster: "c", Namespace: "ns", StateDir: dir}
	if err := os.MkdirAll(dir+"/locks", 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("flock", "-o", Path(e), "sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200 && !Held(e); i++ {
		exec.Command("sleep", "0.01").Run()
	}
	if !Held(e) {
		t.Fatal("child never held the lock")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	release, err := Take(e, "run")
	if err != nil {
		t.Fatalf("after the holder died: %v", err)
	}
	release()
}
