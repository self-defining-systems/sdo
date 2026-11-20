//go:build integration

package bench

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseBlockStatusReadsUnexcludedAndForeignSlices(t *testing.T) {
	st := parseBlockStatus("block 40-55\ntwo-way no\nunexcluded sdo.slice AllowedCPUs= cpuset=0-55\nforeign-pinned team.slice docker-abc.scope\nslice lanes.slice AllowedCPUs=40-55\n")
	if len(st.Unexcluded) != 1 || st.Unexcluded[0] != "sdo.slice" || len(st.Foreign) != 1 || st.Foreign[0] != "team.slice" {
		t.Fatalf("%+v", st)
	}
}

// A sibling slice that does not exclude the block is drift, named, and stops admission. A slice that cannot be shrunk
// because it holds a container pinned inside the block makes the host one-way: no drift, admission goes on (the
// allocator already avoids cores a foreign container is pinned to), and a spec that requires two-way is refused.
func TestUnexcludedSliceDriftsButForeignPinnedOnlyMakesTheHostOneWay(t *testing.T) {
	l := newLab(t)
	m := l.m
	var out string
	m.Helper = func(context.Context, string, ...string) (string, error) { return out, nil }
	m.DriftEvery = time.Nanosecond
	must(t, writeJSON(l.root.reservationPath("h1"), Reservation{Host: "h1", Block: "8-15", TwoWay: true}))
	must(t, m.loadInventory())
	check := func() {
		time.Sleep(time.Millisecond)
		m.checkReservations(context.Background())
	}
	any := Spec{Hosts: []string{"h1"}}
	twoWay := Spec{Hosts: []string{"h1"}, Isolation: IsolationTwoWay}

	out = "block 8-15\ntwo-way yes\nslice lanes.slice AllowedCPUs=8-15\n"
	check()
	if m.drift["h1"] != "" {
		t.Fatalf("%s", m.drift["h1"])
	}
	out = "block 8-15\ntwo-way no\nunexcluded sdo.slice AllowedCPUs= cpuset=0-31\nslice lanes.slice AllowedCPUs=8-15\n"
	check()
	if !strings.Contains(m.drift["h1"], "sdo.slice") {
		t.Fatalf("drift must name the slice that is not excluded: %q", m.drift["h1"])
	}
	if _, _, why := isolationOf(m.inv.Hosts["h1"], any, m.reservations, "h1", m.drift); !strings.Contains(why, "sdo.slice") {
		t.Fatalf("admission stops and says which slice: %q", why)
	}
	out = "block 8-15\ntwo-way no\nforeign-pinned team.slice docker-abc.scope\nslice lanes.slice AllowedCPUs=8-15\n"
	check()
	if m.drift["h1"] != "" {
		t.Fatalf("a foreign pin is not drift: %q", m.drift["h1"])
	}
	mode, block, why := isolationOf(m.inv.Hosts["h1"], any, m.reservations, "h1", m.drift)
	if why != "" || mode != IsolationOneWay || block != "8-15" {
		t.Fatalf("admitted as one-way: %q %q %q", mode, block, why)
	}
	if _, _, why := isolationOf(m.inv.Hosts["h1"], twoWay, m.reservations, "h1", m.drift); !strings.Contains(why, "foreign pinned inside block in team.slice") {
		t.Fatalf("a two-way spec is refused with the slice named: %q", why)
	}
	out = "block 8-15\ntwo-way yes\nslice lanes.slice AllowedCPUs=8-15\n"
	check()
	if mode, _, _ := isolationOf(m.inv.Hosts["h1"], twoWay, m.reservations, "h1", m.drift); mode != IsolationTwoWay {
		t.Fatalf("two-way again once the foreign container is gone: %q", mode)
	}
}
