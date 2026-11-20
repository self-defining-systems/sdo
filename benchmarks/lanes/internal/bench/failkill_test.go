//go:build integration

package bench

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// The kill the manager uses on a hung step really ends the step's process group on the host: found by its step.json
// path, TERM first, and a step that is not there is no error.
func TestLocalHostKillEndsTheStepProcessGroup(t *testing.T) {
	staging := t.TempDir()
	dir := filepath.Join(staging, "c1", "run-1")
	must(t, os.MkdirAll(dir, 0o755))
	path := filepath.Join(dir, "step.json")
	must(t, os.WriteFile(path, []byte("{}"), 0o644))
	// A stand-in for the step: its own session, and a child, with the step.json path on its command line.
	cmd := exec.Command("setsid", "sh", "-c", "sleep 60 & wait # step "+path)
	must(t, cmd.Start())
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	time.Sleep(300 * time.Millisecond) // real: the process must exist before pgrep looks for it
	h := localHost{staging: staging}
	must(t, h.Kill(context.Background(), "c1", "run-1"))
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the step survived Kill")
	}
	must(t, h.Kill(context.Background(), "c1", "run-1")) // already gone: no error
}
