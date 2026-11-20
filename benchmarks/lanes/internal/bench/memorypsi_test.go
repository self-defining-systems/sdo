package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Pressure is measured where our work runs. The memory pressure ("full" avg60) of lanes.slice, the whole lane block,
// over health.mem_psi_full (default 40 %) stops new work on the host and alarms once. Host-wide pressure only raises the
// alarm host-pressure (once per rise) and never gates: it is often another tenant's own memory limit.
func TestMemoryPressureGatesOnTheLaneBlockNotTheHost(t *testing.T) {
	script := []*Canary{
		{MemPSIFull: 12, BlockMemPSIFull: 1},
		{MemPSIFull: 55, BlockMemPSIFull: 0}, // another tenant thrashes: host high, our block quiet
		{MemPSIFull: 60, BlockMemPSIFull: 0},
		{MemPSIFull: 10, BlockMemPSIFull: 65}, // our own block thrashes
		{MemPSIFull: 10, BlockMemPSIFull: 70},
		{MemPSIFull: 10, BlockMemPSIFull: 20},
	}
	m := healthManager(t, &script)
	pass := func() {
		t.Helper()
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	count := func(name string) (n int) {
		for _, a := range RecentAlarms(m.Root, time.Hour, m.now()) {
			if a.Alarm == name {
				n++
			}
		}
		return
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("quiet: %q", m.hostBlock("h1"))
	}
	pass()
	if why := m.hostBlock("h1"); why != "" {
		t.Fatalf("host-wide 55%% with a quiet lane block must admit: %q", why)
	}
	pass()
	if count("host-pressure") != 1 || count("memory-pressure") != 0 {
		t.Fatalf("one host-pressure alarm while it stays high, no gate alarm: %d %d", count("host-pressure"), count("memory-pressure"))
	}
	pass()
	if why := m.hostBlock("h1"); !strings.Contains(why, "lane block") || !strings.Contains(why, "65%") || !strings.Contains(why, "40%") {
		t.Fatalf("lanes.slice 65%% must stop admission and say why: %q", why)
	}
	pass()
	if count("memory-pressure") != 1 {
		t.Fatalf("one gate alarm while it stays high: %d", count("memory-pressure"))
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("block back at 20%%: %q", m.hostBlock("h1"))
	}
	if th := (HostHealthConf{MemPSIFull: 10}).Thresholds(); th.MemPSIFull != 10 {
		t.Fatalf("threshold from hosts.yaml: %+v", th)
	}
}
