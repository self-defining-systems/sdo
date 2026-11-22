//go:build integration

package bench

import (
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
