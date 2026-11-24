//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"os"
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
	if l := m.freeLane(context.Background(), "x", s, reachable); l == nil || l.Lane != "eks2" {
		t.Fatalf("freeLane = %v, want eks2 (holds the hotel copy)", l)
	}
}

func TestALaneHoldingAnotherAppIsRecreatedForTheSpec(t *testing.T) {
	m, s := appManager(t)
	for lane, want := range map[string]bool{"eks1": true, "eks2": false, "eks3": false} {
		if l := m.lanes[lane]; (!l.Gone && !m.holdsApp(context.Background(), l, s)) != want {
			t.Errorf("lane %s: want recreation = %v", lane, want)
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
	if l := m.freeLane(context.Background(), "x", s, map[string]bool{"e": true}); l == nil || l.Lane != "eks2" {
		t.Fatalf("freeLane = %v, want eks2: eks1 is gated", l)
	}
}

func TestFreeLanePrefersMatchingAppContentAcrossCheckoutPins(t *testing.T) {
	m, s := appManager(t)
	checkout := t.TempDir()
	must(t, os.WriteFile(m.Root.path("hosts.yaml"), []byte(eksHost+"checkouts:\n  fuzzops: {new: {e: "+checkout+"}}\n"), 0o644))
	must(t, m.loadInventory())
	host := m.inv.Hosts["e"]
	host.Local = true
	m.inv.Hosts["e"] = host
	s.Fuzzops = "new"
	const content = `{"render":"render-a","images":"images-a","data":"data-a"}`
	adapter := filepath.Join(checkout, "adapter")
	must(t, os.WriteFile(adapter, []byte("#!/bin/sh\n[ \"$1\" = app-content ] || exit 64\necho '"+content+"'\n"), 0o755))
	s.Workload = adapter
	warm := m.lanes["eks2"]
	must(t, json.Unmarshal([]byte(`{"app_content":`+content+`,"workload_rev":"old"}`), warm.Prepared))
	if l := m.freeLane(context.Background(), "x", s, map[string]bool{"e": true}); l != warm {
		t.Fatalf("freeLane = %v, want the matching warm lane %s", l, warm.Lane)
	}
	if !m.holdsApp(context.Background(), warm, s) {
		t.Fatal("matching app content would recreate the cluster")
	}
}

func TestAppReuseTracksEachContentFieldAndKeepsFreshFaultRules(t *testing.T) {
	old := &Prepared{AppPack: "/old/pack", Namespace: "ns", WorkloadRev: "old", Ready: true,
		AppContent: &AppContent{Render: "r", Images: "i", Data: "d", Attachments: "a"}}
	want := &Prepared{AppPack: "/new/pack", Namespace: "ns", WorkloadRev: "new",
		AppContent: &AppContent{Render: "r", Images: "i", Data: "d", Attachments: "different"}}
	s := Spec{ReuseApp: true}
	step := Step{Prev: old, New: want}
	if !reusableApp(s, step, "f@x") {
		t.Fatal("the same deployment should be reusable across checkout and attachment changes")
	}
	for _, tc := range []struct {
		field  string
		change func(*AppContent)
	}{
		{"render", func(c *AppContent) { c.Render = "new" }},
		{"images", func(c *AppContent) { c.Images = "new" }},
		{"data", func(c *AppContent) { c.Data = "new" }},
	} {
		t.Run(tc.field, func(t *testing.T) {
			content := *want.AppContent
			tc.change(&content)
			changed := *want
			changed.AppContent = &content
			if diff := appDifference(old, &changed); diff != tc.field {
				t.Fatalf("app difference = %q, want %q", diff, tc.field)
			}
			if reusableApp(s, Step{Prev: old, New: &changed}, "f@x") {
				t.Fatal("changed deployment would reuse the old copy")
			}
		})
	}
	s.FreshAppFaults = []string{"f@x"}
	if reusableApp(s, step, "f@x") {
		t.Fatal("the requested fault requires a fresh app even when its content matches")
	}
	old.Fault = "f@x"
	if reusableApp(s, step, "g@y") {
		t.Fatal("the previous fault requires a fresh app even when its content matches")
	}
}

func TestClaimRequiresMatchingAppContentAndAttachments(t *testing.T) {
	m, s := appManager(t)
	s.Claim, s.Workload = "pool", "./adapter"
	s.Operator.Hooks, s.Operator.Revision = "subject", "abc"
	m.inv.Checkouts.Hooks = map[string]map[string]map[string]string{"subject": {"abc": {"e": "/subject"}}}
	lane := m.lanes["eks2"]
	lane.State, lane.Experiment = LaneReady, "pool"
	lane.Prepared.WorkloadRev = "old"
	lane.Prepared.AppContent = &AppContent{Render: "r", Images: "i", Data: "d", Attachments: "a"}
	pool := s
	pool.Hold, pool.Fuzzops = true, "old"
	m.exps = map[string]*Experiment{"pool": {Spec: &pool}}
	x := &Experiment{Spec: &s}
	ctx := context.Background()
	for _, tc := range []struct {
		name, render, attachments string
		claim                     bool
	}{
		{"same app across pins", "r", "a", true},
		{"changed render", "changed", "a", false},
		{"changed attachments", "r", "changed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clear(m.appContents)
			m.appContentRunner = func(context.Context, string, string) (string, error) {
				out, err := json.Marshal(AppContent{Render: tc.render, Images: "i", Data: "d", Attachments: tc.attachments})
				return string(out), err
			}
			if got := m.claimable(ctx, x, s); (got == lane) != tc.claim {
				t.Fatalf("claimable = %v, want lane eligible = %v", got, tc.claim)
			}
		})
	}
}
