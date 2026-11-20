package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Memory pressure ("full" avg60) over the threshold (default 40 %) stops new work on the host and alarms once; under it
// the host is admitted again. The threshold is health.mem_psi_full.
func TestMemoryPressureStopsAdmission(t *testing.T) {
	script := []*Canary{{MemPSIFull: 12}, {MemPSIFull: 65}, {MemPSIFull: 70}, {MemPSIFull: 20}}
	m := healthManager(t, &script)
	pass := func() {
		t.Helper()
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("12%%: %q", m.hostBlock("h1"))
	}
	pass()
	if why := m.hostBlock("h1"); !strings.Contains(why, "memory pressure") || !strings.Contains(why, "65%") || !strings.Contains(why, "40%") {
		t.Fatalf("65%% must stop admission and say why: %q", why)
	}
	pass()
	n := 0
	for _, a := range RecentAlarms(m.Root, time.Hour, time.Now()) {
		if a.Alarm == "memory-pressure" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("one alarm while it stays high: %d", n)
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("20%%: %q", m.hostBlock("h1"))
	}
	if th := (HostHealthConf{MemPSIFull: 10}).Thresholds(); th.MemPSIFull != 10 {
		t.Fatalf("threshold from hosts.yaml: %+v", th)
	}
}
