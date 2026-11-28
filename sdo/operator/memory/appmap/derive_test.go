package appmap

import (
	"reflect"
	"strings"
	"testing"
)

// A probe line states a scenario's kind once: workload files that omit the
// scenario still run it in the workload of its kind, and the map checks pass.
func TestProbeLinesPlaceScenariosInTheirWorkload(t *testing.T) {
	m := mustParse(t, shopYAML)
	empty := []Workload{{Name: "probe-call"}, {Name: "probe-write"}, {Name: "probe-read-later"}}
	want := mustParse(t, shopYAML).Check(ParseInventory(shopInventory), shopWorkloads)
	if got := m.Check(ParseInventory(shopInventory), empty); !reflect.DeepEqual(rules(got), rules(want)) {
		t.Fatalf("omitted lists: %v, listed: %v", rules(got), rules(want))
	}
	placed := m.WithProbeMembership(empty)
	if !reflect.DeepEqual(placed, shopWorkloads) {
		t.Fatalf("placed %v", placed)
	}
	// A scenario listed in the wrong workload is not moved: still an error.
	wrong := []Workload{{Name: "probe-call", Scenarios: []string{"write-item"}}, {Name: "probe-write"}, {Name: "probe-read-later"}}
	found := false
	for _, f := range m.Check(ParseInventory(shopInventory), wrong) {
		found = found || strings.Contains(f.Message, "probe write-item has kind write-read but runs in workload probe-call")
	}
	if !found {
		t.Fatal("a scenario in the wrong workload stays an error")
	}
}

// Each probe's path gives one Service list per component, in path order.
func TestProbePathsListEachComponentsServices(t *testing.T) {
	got := mustParse(t, shopYAML).ProbePaths()["items-later"]
	if !reflect.DeepEqual(got, [][]string{{"web"}, {"api"}, {"db", "db-headless"}}) {
		t.Fatalf("paths %v", got)
	}
}

// Data that only its writer reaches cannot be read back later by any probe,
// so it is no read-later obligation; data another operation reads still is.
func TestUncoveredDataNeedsAnOperationThatReadsTheData(t *testing.T) {
	text := strings.Replace(shopYAML, "    path: [web, api, db]\n  - id: add-item", "    path: [web, api]\n  - id: add-item", 1)
	text = strings.Replace(text, "  - {scenario: items-later, kind: read-later, operations: [list-items]}\n", "", 1)
	workloads := []Workload{{Name: "probe-call", Scenarios: []string{"call-all"}}, {Name: "probe-write", Scenarios: []string{"write-item"}}, {Name: "probe-read-later"}}
	if got := ruleMessages(mustParse(t, text).Check(ParseInventory(shopInventory), workloads), RuleUncoveredData); len(got) != 0 {
		t.Fatalf("data only its writer reaches raised %v", got)
	}
	read := strings.Replace(shopYAML, "  - {scenario: items-later, kind: read-later, operations: [list-items]}\n", "", 1)
	if got := ruleMessages(mustParse(t, read).Check(ParseInventory(shopInventory), workloads), RuleUncoveredData); len(got) != 1 {
		t.Fatalf("data a read operation reaches must still be read later: %v", got)
	}
}
