//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// stepWakeManager is a manager with one lane whose step runs on a local host.
func stepWakeManager(t *testing.T, tick time.Duration) (*Manager, string) {
	t.Helper()
	staging := t.TempDir()
	step := filepath.Join(staging, "l1", "run-1")
	if err := os.MkdirAll(step, 0o755); err != nil {
		t.Fatal(err)
	}
	m := &Manager{
		Root: Root(t.TempDir()),
		Tick: tick,
		inv:  Inventory{Hosts: map[string]Host{"h": {Local: true, Staging: staging}}},
		lanes: map[string]*LaneFile{
			"l1": {Lane: "l1", Host: "h", Step: &StepRef{ID: "run-1", Kind: StepRun}},
		},
	}
	return m, step
}

// A step on a local host that writes its manifest ends the wait for the next pass: the manager adopts it within a
// poll, not after its tick.
func TestAFinishedLocalStepEndsTheWaitForTheNextPass(t *testing.T) {
	m, step := stepWakeManager(t, time.Hour)
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = os.WriteFile(filepath.Join(step, "manifest.json"), []byte("{}"), 0o644)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := time.Now()
	if !m.waitTick(ctx) {
		t.Fatalf("the manager was still waiting for its tick %s after a local step finished", time.Since(start))
	}
}

// A finished step that a pass did not adopt (it is retried next pass) must not turn the wait into a spin: the manager
// wakes for each finished step once and otherwise keeps to its tick.
func TestAFinishedStepWakesTheManagerOnce(t *testing.T) {
	m, step := stepWakeManager(t, 1500*time.Millisecond)
	if err := os.WriteFile(filepath.Join(step, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !m.waitTick(context.Background()) {
		t.Fatal("waitTick said the context ended")
	}
	start := time.Now()
	if !m.waitTick(context.Background()) {
		t.Fatal("waitTick said the context ended")
	}
	if took := time.Since(start); took < time.Second {
		t.Fatalf("the manager passed again after %s for a step it had already been woken for", took)
	}
}

// A step on a host reached over ssh is seen by the probe of the next pass only: its manifest is not on this machine.
func TestARemoteStepDoesNotWakeTheManager(t *testing.T) {
	m, step := stepWakeManager(t, 1200*time.Millisecond)
	m.inv.Hosts["h"] = Host{SSH: "elsewhere", Staging: m.inv.Hosts["h"].Staging}
	if err := os.WriteFile(filepath.Join(step, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if !m.waitTick(context.Background()) {
		t.Fatal("waitTick said the context ended")
	}
	if took := time.Since(start); took < time.Second {
		t.Fatalf("the manager passed after %s for a step it cannot see", took)
	}
}
