package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestKernelMinimumsDefaultsAndOverrides(t *testing.T) {
	d := Host{}.KernelMins()
	if d["fs.inotify.max_user_instances"] != 8192 || d["fs.inotify.max_user_watches"] != 1048576 || d["vm.max_map_count"] != 262144 || len(d) != 3 {
		t.Fatalf("defaults: %v", d)
	}
	h := Host{KernelMinimums: map[string]int64{"vm.max_map_count": 1 << 20, "fs.file-max": 100}}
	m := h.KernelMins()
	if m["vm.max_map_count"] != 1<<20 || m["fs.file-max"] != 100 || m["fs.inotify.max_user_instances"] != 8192 {
		t.Fatalf("a host adds to and overrides the defaults: %v", m)
	}
	inv := Inventory{Hosts: map[string]Host{"h": {Local: true, Staging: "/s", Kubeconfigs: "/k", MaxBuilds: 1, MaxLoad5: 1, KernelMinimums: map[string]int64{"vm.x; rm -rf /": 1}}}}
	if err := inv.Validate(); err == nil || !strings.Contains(err.Error(), "kernel_minimums") {
		t.Fatalf("a key that is not a sysctl name must be refused: %v", err)
	}
}

func TestKernelLowNamesTheSettingAndWhereItIsOverridden(t *testing.T) {
	mins := Host{}.KernelMins()
	c := &Canary{Sysctl: map[string]int64{"fs.inotify.max_user_instances": 512, "fs.inotify.max_user_watches": 2 << 20, "vm.max_map_count": 262144},
		Override: map[string]string{"fs.inotify.max_user_instances": "/etc/sysctl.conf:12:fs.inotify.max_user_instances=512"}}
	why := kernelLow(c, mins)
	for _, want := range []string{"fs.inotify.max_user_instances is 512", "below the minimum 8192", "/etc/sysctl.conf:12"} {
		if !strings.Contains(why, want) {
			t.Fatalf("%q lacks %q", why, want)
		}
	}
	if strings.Contains(why, "watches") || strings.Contains(why, "max_map_count") {
		t.Fatalf("only the settings below their minimum: %q", why)
	}
	if kernelLow(&Canary{Sysctl: map[string]int64{"vm.max_map_count": 1 << 30}}, mins) != "" {
		t.Fatal("a setting at or above its minimum, or not read, is fine")
	}
}

// A host whose effective kernel settings are below the minimums is not admitted, with the reason; an alarm fires when a
// value drops (and once per drop); a shared RDMA netns is a warning only.
func TestKernelSettingBelowMinimumStopsAdmissionAndAlarms(t *testing.T) {
	low := &Canary{Sysctl: map[string]int64{"fs.inotify.max_user_instances": 512}, Override: map[string]string{"fs.inotify.max_user_instances": "/etc/sysctl.conf:7:x"}}
	ok := &Canary{Sysctl: map[string]int64{"fs.inotify.max_user_instances": 8192}}
	rdma := &Canary{Sysctl: map[string]int64{"fs.inotify.max_user_instances": 8192}, RDMAShared: true}
	script := []*Canary{ok, low, low, ok, low, rdma}
	m := healthManager(t, &script)
	pass := func() {
		t.Helper()
		if err := m.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	alarms := func() int {
		n := 0
		for _, a := range RecentAlarms(m.Root, time.Hour, m.now()) {
			if a.Alarm == "kernel-setting-low" {
				n++
			}
		}
		return n
	}
	pass()
	if m.hostBlock("h1") != "" || alarms() != 0 {
		t.Fatalf("fine: %q", m.hostBlock("h1"))
	}
	pass()
	if why := m.hostBlock("h1"); !strings.Contains(why, "max_user_instances is 512") || !strings.Contains(why, "/etc/sysctl.conf:7") {
		t.Fatalf("not admitted, and why: %q", why)
	}
	if got := m.hosts["h1"].Unhealthy; !strings.Contains(got, "max_user_instances") {
		t.Fatalf("lanes status shows the reason: %q", got)
	}
	pass()
	if alarms() != 1 {
		t.Fatalf("one alarm while it stays low: %d", alarms())
	}
	pass()
	if m.hostBlock("h1") != "" {
		t.Fatalf("admitted again once the value is back: %q", m.hostBlock("h1"))
	}
	pass()
	if alarms() != 2 {
		t.Fatalf("a drop while lanes may be running alarms again: %d", alarms())
	}
	pass()
	if m.hostBlock("h1") != "" || !strings.Contains(strings.Join(m.hosts["h1"].Warnings, ";"), "RDMA") {
		t.Fatalf("shared RDMA is a warning, never a gate: block %q warnings %v", m.hostBlock("h1"), m.hosts["h1"].Warnings)
	}
}
