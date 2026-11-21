//go:build integration

package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// waitingPlan adds a plan of one item with arms p1 and p2 (edge p1 -> p2, both on the given hosts, a JSON list body) to
// the manager's root and leaves it as a p1 retry in progress: p1 is back to waiting after a failed attempt, p2 is
// blocked on p1's artifact.
func waitingPlan(t *testing.T, m *Manager, hosts, selector string) {
	t.Helper()
	dir := t.TempDir()
	tpl := `{"app": "examples/shop", "operator": {"hooks": "test"}, "namespace": "app1",
 "faults": {"instances": "instances.json", "ids": ["a@x"]}, "sequences": [[1]], "run": {"window": "1s"}, "ready_for": "100ms",
 "ready_timeout": "1s", "cap": "10m", "lanes": 1, "hosts": [` + hosts + `]` + selector + `}`
	must(t, os.WriteFile(filepath.Join(dir, "p1.json"), []byte(tpl), 0o644))
	must(t, os.WriteFile(filepath.Join(dir, "p2.json"), []byte(tpl), 0o644))
	file := filepath.Join(dir, "plan.yaml")
	must(t, os.WriteFile(file, []byte(`plan: waits
pins: {workload: fuzzops@abc, subject: test@abcdef0}
arms: [{name: p1, spec: p1.json}, {name: p2, spec: p2.json}]
items: [a]
artifacts: {rec: {path: "${run}/run.json"}}
edges: [{from: p1, to: p2, artifact: rec}]
`), 0o644))
	// The plan pins full revisions; the host has a checkout of the workload at that one.
	const fz, hooks = "0b528ac000000000000000000000000000000000", "abcdef0000000000000000000000000000000000"
	revs := map[string]string{"abc": fz, "abcdef0": hooks}
	must(t, os.WriteFile(m.Root.path("hosts.yaml"), []byte(eksHost+"checkouts:\n  fuzzops: {"+fz+": {e: /fz}}\n  hooks:\n    test: {"+hooks+": {e: /h}}\nhook_command: {test: \"true\"}\n"), 0o644))
	must(t, m.loadInventory())
	if _, err := plan.Add(m.Root.PlansDir(), file, SystemClock{}, nil, plan.WithResolver(func(_, ref string) (string, error) { return revs[ref], nil })); err != nil {
		t.Fatal(err)
	}
	pd := plan.Dir(m.Root.PlansDir(), "waits")
	st, err := plan.LoadState(pd)
	must(t, err)
	st.Nodes[0].Status, st.Nodes[0].Attempt, st.Nodes[0].Reason = plan.NodeWaiting, 1, "lane-lost" // p1: failed once, retry pending
	must(t, writeJSON(filepath.Join(pd, "state.json"), st))
}

// A p1 retry that outlasts idle_timeout must not cost p2 its lane: while a plan has nodes waiting (queued, blocked on an
// edge or retrying) that the lane can serve, the lane is kept past idle_timeout.
func TestEksKeepsAFreeLaneWhileAPlanHasWaitingWorkForIt(t *testing.T) {
	m := eksManager(t)
	waitingPlan(t, m, `"e"`, "")
	l := &LaneFile{Lane: "eks1", Host: "e", Since: m.now()}
	m.advance(6 * time.Minute) // idle_timeout is 5m
	if m.eksIdle(l) {
		t.Fatal("the lane is idle after 6m although p1 retries and p2 waits on it: p2 would build a new cluster")
	}
}

// A plan stalled for ever must not hold a lane for ever: at max_idle_with_waiting (default 60m) the lane goes and the
// alarm idle-ceiling says why. The alarm is raised once.
func TestEksDeletesAFreeLaneAtTheCeilingAndAlarms(t *testing.T) {
	m := eksManager(t)
	waitingPlan(t, m, `"e"`, "")
	l := &LaneFile{Lane: "eks1", Host: "e", Since: m.now()}
	m.advance(59 * time.Minute)
	if m.eksIdle(l) {
		t.Fatal("the lane went before the ceiling (60m)")
	}
	m.advance(time.Minute)
	if !m.eksIdle(l) || !m.eksIdle(l) {
		t.Fatal("the lane was kept past the ceiling")
	}
	got := RecentAlarms(m.Root, time.Hour, m.now())
	if len(got) != 1 || got[0].Alarm != "idle-ceiling" || got[0].Subject != "e/eks1" {
		t.Fatalf("alarms = %+v, want one idle-ceiling for e/eks1", got)
	}
}

// Waiting work that the lane cannot serve (another selector) does not keep it; and without any plan the idle timeout
// alone decides, as before.
func TestEksIdleTimeoutStillAppliesWhenNoWaitingWorkFitsTheLane(t *testing.T) {
	m := eksManager(t)
	l := &LaneFile{Lane: "eks1", Host: "e", Since: m.now()}
	m.advance(6 * time.Minute)
	if !m.eksIdle(l) {
		t.Fatal("no plan: the idle timeout (5m) passed")
	}
	waitingPlan(t, m, `"e"`, `, "selector": {"workload": "other"}`)
	if !m.eksIdle(l) {
		t.Fatal("a waiting node whose selector the lane does not satisfy kept it")
	}
}

func TestEksMaxIdleWithWaitingIsValidated(t *testing.T) {
	_, err := loadInv(t, strings.Replace(eksHost, "idle_timeout: 5m", "idle_timeout: 5m, max_idle_with_waiting: soon", 1))
	if err == nil || !strings.Contains(err.Error(), "max_idle_with_waiting") {
		t.Fatalf("err = %v", err)
	}
}
