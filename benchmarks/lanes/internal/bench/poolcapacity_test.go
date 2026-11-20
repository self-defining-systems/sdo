//go:build integration

package bench

import (
	"os"
	"strings"
	"testing"
)

// A pool (hold) keeps one ready lane per slot. Slots the hosts' lane ranges cannot place would build forever,
// waiting for cores while holding pool slots, so `add` refuses a pool larger than the placeable capacity.
func TestAddRefusesAPoolLargerThanThePlaceableCapacity(t *testing.T) {
	l := newFlab(t, "h1=c1", "h2=c2")
	l.extra["h1"] = `, lanes: {cpus: "0-15"}`
	l.extra["h2"] = `, lanes: {cpus: "0-15"}`
	l.specExtra = "hold: true\nlane: {cpus: 8, memory: 10Gi}\n"
	l.writeInventory()
	// 4 slots on h1 alone: 16 cores / 8 per lane = 2.
	err := l.addErr("pool", []string{"h1"}, "a@x", 0)
	if err == nil {
		t.Fatal("a pool of 4 slots was added to a host that places 2")
	}
	for _, want := range []string{"4 slots", "h1", "16 cores", "8 per lane", "2"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
	if _, serr := os.Stat(l.root.SpecPath("pool")); serr == nil {
		t.Fatal("the refused pool was added")
	}
	// Both hosts place 4 together.
	if err := l.addErr("pool", []string{"h1", "h2"}, "a@x", 0); err != nil {
		t.Fatalf("a pool that fits was refused: %v", err)
	}
}

// Only a pool holds its slots while waiting for cores; an ordinary experiment's extra lanes just queue.
func TestAddAllowsAnExperimentLargerThanTheCapacity(t *testing.T) {
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = `, lanes: {cpus: "0-15"}`
	l.specExtra = "lane: {cpus: 8, memory: 10Gi}\n"
	l.writeInventory()
	if err := l.addErr("e1", []string{"h1"}, "a@x", 0); err != nil {
		t.Fatalf("an experiment was refused: %v", err)
	}
}
