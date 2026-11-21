//go:build integration

package bench

import (
	"os"
	"strings"
	"testing"
)

const labeledEksHost = `hosts:
  e:
    local: true
    provider: eks
    eks: {region: us-west-2, account: "123456789012", budget_per_hour: 2.0, budget_total: 10, vcpu_quota: 24, max_lanes: 3, idle_timeout: 5m}
    staging: /s
    kubeconfigs: /k
    evidence: /e
    max_builds: 2
    clusters: [eks1, eks2, eks3]
    lane_labels:
      eks1: {workload: hotel}
      eks2: {workload: gitlab}
`

// wantedManager is an eks manager whose lanes carry labels (eks1 hotel, eks2 gitlab, eks3 none) and which has one
// unfinished experiment with the given selector on the host.
func wantedManager(t *testing.T, selector map[string]string) *Manager {
	t.Helper()
	m := eksManager(t)
	must(t, os.WriteFile(m.Root.path("hosts.yaml"), []byte(labeledEksHost+"checkouts:\n  fuzzops: {abc: {e: /fz}}\n"), 0o644))
	must(t, m.loadInventory())
	s := Spec{Name: "x", Fuzzops: "abc", Hosts: []string{"e"}, Selector: selector}
	s.Faults.IDs = []string{"a@x"}
	s.Sequences = [][]int{{1}}
	m.exps = map[string]*Experiment{"x": {Name: "x", Spec: &s, Lock: Lock{Spec: s}}}
	return m
}

// "Wanted" was host-wide: any unfinished experiment on the host kept every lane of it, so the hotel lanes of a batch
// whose remaining work was gitlab billed for an hour. A lane is wanted only by a waiting experiment that can be placed
// on it.
func TestAWaitingExperimentWantsOnlyTheLanesItCanBePlacedOn(t *testing.T) {
	m := wantedManager(t, map[string]string{"workload": "gitlab"})
	for lane, want := range map[string]bool{"eks1": false, "eks2": true, "eks3": false} {
		if got := m.wantedBy(m.lanes[lane]); got != want {
			t.Errorf("lane %s (gitlab work waiting): wanted = %v, want %v", lane, got, want)
		}
	}
	m = wantedManager(t, nil) // no selector: only the unlabeled lane takes it
	for lane, want := range map[string]bool{"eks1": false, "eks2": false, "eks3": true} {
		if got := m.wantedBy(m.lanes[lane]); got != want {
			t.Errorf("lane %s (unselected work waiting): wanted = %v, want %v", lane, got, want)
		}
	}
}

func TestAnExperimentOnAnotherHostWantsNoLaneOfThisOne(t *testing.T) {
	m := wantedManager(t, nil)
	m.exps["x"].Spec.Hosts = []string{"other"}
	if m.wantedBy(m.lanes["eks3"]) {
		t.Fatal("an experiment that cannot use this host keeps its lane")
	}
}

// `lane release LANE` frees a lane's cluster now, whatever waits and however long it has been free; it refuses a lane
// that is not free.
func TestLaneReleaseRequestDeletesAFreeLaneNow(t *testing.T) {
	m := wantedManager(t, map[string]string{"workload": "hotel"})
	l := m.lanes["eks1"]
	l.Gone = false
	must(t, writeJSON(m.Root.LanePath("eks1"), l))
	if m.releaseRequested(l) {
		t.Fatal("a release nobody asked for")
	}
	must(t, LaneRelease(m.Root, "eks1"))
	if !m.releaseRequested(l) {
		t.Fatal("the request is not seen")
	}
	m.consumeRelease(l)
	if m.releaseRequested(l) {
		t.Fatal("a consumed request is still there")
	}
	l.State = LaneReady
	must(t, writeJSON(m.Root.LanePath("eks1"), l))
	if err := LaneRelease(m.Root, "eks1"); err == nil || !strings.Contains(err.Error(), "not free") {
		t.Fatalf("a busy lane: %v", err)
	}
}
