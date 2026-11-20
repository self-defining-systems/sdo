package bench

import (
	"context"
	"strings"
	"testing"
)

// The gate survives a manager restart: the hung-task baseline (so tasks that hung while the manager was down show as new
// at its first probe, and a count that merely carries over does not) and an unhealthy host's reason and good-probe count.
func TestHealthStateSurvivesAManagerRestart(t *testing.T) {
	script := []*Canary{good(5, 4)}
	m := healthManager(t, &script)
	if err := m.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	restart := func(c *Canary) *Manager {
		t.Helper()
		script = []*Canary{c}
		m2 := &Manager{Root: m.Root, Ops: m.Ops}
		if err := m2.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		return m2
	}
	if m2 := restart(good(5, 4)); m2.hostBlock("h1") != "" {
		t.Fatalf("an unchanged count is not new hung tasks: %q", m2.hostBlock("h1"))
	}
	if m3 := restart(good(5, 7)); !strings.Contains(m3.hostBlock("h1"), "3 new hung task") {
		t.Fatalf("tasks that hung while the manager was down are new at its first probe: %q", m3.hostBlock("h1"))
	}
	m4 := restart(good(5, 7))
	if m4.hostBlock("h1") == "" {
		t.Fatal("a restart must not clear an unhealthy host")
	}
	for i := 0; i < 2; i++ {
		script = []*Canary{good(5, 7)}
		if err := m4.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if m4.hostBlock("h1") != "" {
		t.Fatalf("three good probes in total, across the restart, recover the host: %q", m4.hostBlock("h1"))
	}
}
