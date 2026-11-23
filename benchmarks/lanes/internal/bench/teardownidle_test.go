//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The idle gate before a teardown is not an inject gate: the operator is about to be deleted, so what an inject needs
// (a subject's store baselines, say) is no reason to wait. The hook is told why it is asked, because on its own it
// cannot tell a release's idle from a run's pre-inject gate (EKS batch B: release waited 10 to 18 minutes for a
// baseline nothing would use).
func TestTeardownIdleTellsTheHookItIsForATeardown(t *testing.T) {
	dir := t.TempDir()
	seen := filepath.Join(dir, "seen")
	hook := filepath.Join(dir, "hook.sh")
	script := `#!/bin/sh
case $1 in
  idle) echo "${BENCH_IDLE_FOR:-gate}" >> "` + seen + `"; [ "$BENCH_IDLE_FOR" = teardown ] || { echo "blind: no promoted baseline yet for store"; exit 1; } ;;
esac
`
	must(t, os.WriteFile(hook, []byte(script), 0o755))
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
	r := &stepRunner{dir: t.TempDir(), poll: time.Millisecond, hookLog: io.Discard}
	r.s.Cap = Seconds(time.Minute.Seconds())
	start := time.Now()
	if !r.waitIdle(context.Background(), p) {
		t.Fatal("waitIdle failed")
	}
	if time.Since(start) > 10*time.Second {
		t.Fatalf("waitIdle held %s on a condition only an inject needs", time.Since(start))
	}
	b, _ := os.ReadFile(seen)
	if got := strings.Fields(string(b)); len(got) != 2 || got[0] != "teardown" || got[1] != "teardown" {
		t.Fatalf("idle calls were made for %q, want teardown twice", got)
	}
}

// A teardown's idle wait says what it waits for while it waits: a release that sat two hours in this gate left
// nothing in its log to name the reason (the step directory was all there was, and the next release replaced it).
func TestTeardownIdleLogsWhatItWaitsForAndWhenItGivesUp(t *testing.T) {
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook.sh")
	must(t, os.WriteFile(hook, []byte("#!/bin/sh\necho 'alarm: incident open fg1-1'\nexit 1\n"), 0o755))
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
	var log strings.Builder
	r := &stepRunner{dir: t.TempDir(), poll: 5 * time.Millisecond, hookLog: io.Discard, log: &log}
	r.s.Cap = Seconds(0.2)
	old := idleProgressEvery
	idleProgressEvery = 20 * time.Millisecond
	defer func() { idleProgressEvery = old }()
	if !r.waitIdle(context.Background(), p) {
		t.Fatal("a capped idle wait must go on to the teardown")
	}
	got := log.String()
	if strings.Count(got, "idle: still waiting") < 2 || !strings.Contains(got, "alarm: incident open fg1-1") {
		t.Errorf("no progress naming the reason: %q", got)
	}
	if !strings.Contains(got, "idle: gave up") {
		t.Errorf("giving up at the cap is not logged: %q", got)
	}
}

// Two calm answers end a teardown's idle wait, and the second only has to come after the same short pause the settle
// uses, not after the step's whole idle poll: with a 10 s poll every release spent 11.1 s on two answers of an
// operator whose idle is a constant.
func TestTeardownIdleRechecksACalmOperatorAfterAShortPause(t *testing.T) {
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook.sh")
	must(t, os.WriteFile(hook, []byte("#!/bin/sh\nexit 0\n"), 0o755))
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
	r := &stepRunner{dir: t.TempDir(), poll: 30 * time.Second, hookLog: io.Discard}
	r.s.Cap = Seconds(time.Minute.Seconds())
	start := time.Now()
	if !r.waitIdle(context.Background(), p) {
		t.Fatal("waitIdle failed")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("a calm operator took %s to be idle before a teardown with a 30 s poll, want the second answer after the short recheck pause", took)
	}
}

// A busy answer is still polled at the step's poll: only a calm one is rechecked early.
func TestTeardownIdlePollsABusyOperatorAtThePoll(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "busy-once")
	hook := filepath.Join(dir, "hook.sh")
	must(t, os.WriteFile(hook, []byte("#!/bin/sh\nif [ ! -e "+flag+" ]; then touch "+flag+"; echo 'alarm: busy'; exit 1; fi\nexit 0\n"), 0o755))
	p := &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
	r := &stepRunner{dir: t.TempDir(), poll: 1500 * time.Millisecond, hookLog: io.Discard}
	r.s.Cap = Seconds(time.Minute.Seconds())
	start := time.Now()
	if !r.waitIdle(context.Background(), p) {
		t.Fatal("waitIdle failed")
	}
	if took := time.Since(start); took < 1500*time.Millisecond {
		t.Fatalf("a busy answer was asked again after %s, less than the poll", took)
	}
}
