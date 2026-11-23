//go:build integration

package bench

import (
	"context"
	"path/filepath"
	"testing"
)

// appManager is an eks manager with three free lanes whose clusters exist: eks1 holds the astro copy, eks2 the hotel
// copy, eks3 is gone (it is not a candidate here: freeLane tries lanes that hold cores first); and the spec of a hotel experiment.
func appManager(t *testing.T) (*Manager, Spec) {
	t.Helper()
	m := eksManager(t)
	s := Spec{Name: "x", Fuzzops: "abc", Hosts: []string{"e"}, App: "apps/hotel", Namespace: "hotel"}
	s.Faults.IDs = []string{"a@x"}
	s.Sequences = [][]int{{1}}
	held := func(app, ns string) *Prepared {
		return &Prepared{Experiment: "old", Namespace: ns, AppPack: filepath.Join("/fz", app), Ready: true}
	}
	m.lanes["eks1"].Gone, m.lanes["eks1"].Prepared = false, held("apps/astro", "astro")
	m.lanes["eks2"].Gone, m.lanes["eks2"].Prepared = false, held("apps/hotel", "hotel")
	return m, s
}

// Any lane may run any app, but a lane that holds another app's cluster must be made anew for it (a failed build, then a
// rebuild): a free lane that already holds the spec's app copy is taken first, even when another sorts before it.
func TestAFreeLaneHoldingTheSpecsAppIsTakenBeforeOthers(t *testing.T) {
	m, s := appManager(t)
	reachable := map[string]bool{"e": true}
	if l := m.freeLane("x", s, reachable); l == nil || l.Lane != "eks2" {
		t.Fatalf("freeLane = %v, want eks2 (holds the hotel copy)", l)
	}
}

func TestALaneHoldingAnotherAppIsRecreatedForTheSpec(t *testing.T) {
	m, s := appManager(t)
	for lane, want := range map[string]bool{"eks1": true, "eks2": false, "eks3": false} {
		if got := m.holdsOtherApp(m.lanes[lane], s); got != want {
			t.Errorf("lane %s: holdsOtherApp = %v, want %v", lane, got, want)
		}
	}
}

// A lane whose cluster was unreachable while it was being released keeps that reading as its gate for ever: observeEksLanes
// skips a Gone lane, so nothing cleared it and every build on the lane "could not start" (17 lanes of a pool sat idle for
// 25 minutes with free clusters nowhere and the pool queued).
func TestAGoneLaneIsNotGatedByAnEarlierUnreachableReading(t *testing.T) {
	m := eksManager(t) // eks1..eks3 are Gone
	m.eksWhy = map[string]string{"eks1": "lane eks1 is unhealthy: EKS API not reachable: exit status 1, no new steps on it"}
	m.observeEksLanes(context.Background(), map[string]bool{"e": true})
	if why := m.laneGate("eks1"); why != "" {
		t.Errorf("a Gone lane is gated: %q", why)
	}
}

// A free lane the manager will not start a build on (its cluster is unreachable) must not be the one a build is offered:
// freeLane took the first lane that holds cores, a gated lane among them, the start was refused, and the pass ended, so
// 20 lanes with no cluster waited behind one lane whose cluster was already gone.
func TestFreeLaneSkipsAGatedLane(t *testing.T) {
	m, s := appManager(t)
	m.lanes["eks2"].Prepared = nil // no lane holds the app: the choice is by lane order
	m.lanes["eks1"].Gone, m.lanes["eks2"].Gone = false, false
	m.eksWhy = map[string]string{"eks1": "lane eks1 is unhealthy: EKS API not reachable: exit status 1, no new steps on it"}
	if l := m.freeLane("x", s, map[string]bool{"e": true}); l == nil || l.Lane != "eks2" {
		t.Fatalf("freeLane = %v, want eks2: eks1 is gated", l)
	}
}
