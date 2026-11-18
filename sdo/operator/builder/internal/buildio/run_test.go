package buildio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeChild(t *testing.T, body string) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "child.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	return script
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never appeared", path)
}

// A stopped controller must run its own shutdown (the leader Lease release)
// before it is gone: cancelling the parent's context sends SIGTERM and waits
// for the child instead of killing it.
func TestCancelledRunLetsTheChildRunItsShutdown(t *testing.T) {
	dir := t.TempDir()
	ready, released := filepath.Join(dir, "ready"), filepath.Join(dir, "released")
	child := writeChild(t, "trap 'touch "+released+"; exit 0' TERM\ntouch "+ready+"\nwhile :; do sleep 0.05; done\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runController(ctx, child, "unused.json", dir, 5*time.Second) }()
	waitForFile(t, ready)
	cancel()
	select {
	case err := <-done:
		// exec reports a clean exit after cancellation as ctx.Err(); the caller
		// treats that as a normal stop, never as a failed child.
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a child that stops on SIGTERM must end the run as a cancellation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after the child handled SIGTERM")
	}
	if _, err := os.Stat(released); err != nil {
		t.Fatalf("child was not given the chance to shut down: %v", err)
	}
}

// A child that ignores SIGTERM is still ended, once the grace has passed.
func TestCancelledRunKillsAChildThatIgnoresTermination(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	child := writeChild(t, "trap '' TERM\ntouch "+ready+"\nwhile :; do sleep 0.05; done\n")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runController(ctx, child, "unused.json", dir, 300*time.Millisecond) }()
	waitForFile(t, ready)
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a killed child is a failed run")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run never returned: a child ignoring SIGTERM was not killed")
	}
	if elapsed := time.Since(started); elapsed < 250*time.Millisecond {
		t.Fatalf("child was killed after %v, before its grace", elapsed)
	}
}

func TestStopGraceIsBoundedBelowThePodGracePeriod(t *testing.T) {
	if StopGrace <= 5*time.Second || StopGrace >= 30*time.Second {
		t.Fatalf("StopGrace = %v; it must exceed the Lease release (5s) and fit the pod grace period", StopGrace)
	}
}

// A second signal on the parent does not wait out the grace: the forced-stop
// channel kills the child at once.
func TestForcedStopKillsTheChildBeforeItsGrace(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	child := writeChild(t, "trap '' TERM\ntouch "+ready+"\nwhile :; do sleep 0.05; done\n")
	force := make(chan struct{})
	ctx, cancel := context.WithCancel(WithForcedStop(context.Background(), force))
	done := make(chan error, 1)
	go func() { done <- runController(ctx, child, "unused.json", dir, time.Minute) }()
	waitForFile(t, ready)
	cancel()
	select {
	case <-done:
		t.Fatal("the child was ended before the forced stop")
	case <-time.After(300 * time.Millisecond):
	}
	close(force)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a killed child is a failed run")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("forced stop did not kill the child")
	}
}
