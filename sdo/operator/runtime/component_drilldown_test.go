package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
)

// componentLoop is the on-demand loop over the detect fixture with the
// accepted architecture (mapTestDoc) behind the read view.
func componentLoop(t *testing.T, appState AppStateChangeSource, doc string) *ResponderLoop {
	t.Helper()
	loop, _ := onDemandLoop(t, detectApp("create"), appState)
	loop.Request.DetectedAt = detectNow().Add(-time.Minute)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
		return mapView{ReadView: program.ReadFrom(v), doc: doc}
	}
	return loop
}

func TestChangesComponentKeepsOnlyThatComponentsRows(t *testing.T) {
	onset := detectNow().Add(-time.Minute)
	rows := []IncidentChange{
		{At: onset.Add(-time.Minute), Component: "db", Source: "app-state", ReaderOrObject: "settings", Key: "rate_limit", Before: "1", After: "2"},
		{At: onset.Add(-time.Minute), Component: "jobs", Source: "app-state", ReaderOrObject: "cron", Key: "interval", Before: "1", After: "2"},
		{At: onset.Add(-time.Minute), Source: "app-state", ReaderOrObject: "limits", Key: "unplaced", Before: "1", After: "2"},
	}
	loop := componentLoop(t, &staticAppState{rows: rows}, mapTestDoc)
	response := loop.Handle(context.Background(), ToolRequest{Command: "changes", Components: []string{"db"}})
	if response.Error != "" || !strings.Contains(response.Text, "rate_limit") || strings.Contains(response.Text, "interval") || strings.Contains(response.Text, "unplaced") {
		t.Fatalf("--component db: %q, error %q", response.Text, response.Error)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "changes", Components: []string{"db,jobs"}})
	if !strings.Contains(response.Text, "rate_limit") || !strings.Contains(response.Text, "interval") {
		t.Fatalf("a comma list keeps both:\n%s", response.Text)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "changes"})
	if !strings.Contains(response.Text, "unplaced") {
		t.Fatalf("an unplaced row is reachable unfiltered:\n%s", response.Text)
	}
}

func TestComponentNamesAreValidatedAgainstTheMap(t *testing.T) {
	loop := componentLoop(t, &staticAppState{}, mapTestDoc)
	for _, command := range []string{"changes", "timeline"} {
		response := loop.Handle(context.Background(), ToolRequest{Command: command, Components: []string{"nope"}})
		if !strings.Contains(response.Error, `unknown component "nope"; components: api, cache, db, front, jobs, mailer`) {
			t.Fatalf("%s: %q", command, response.Error)
		}
	}
	// With no readable map at all there is nothing to validate against: the
	// names filter whatever Component the rows carry.
	view := detectApp("create")
	view.ApplicationMap = nil
	loop, _ = onDemandLoop(t, view, &staticAppState{rows: []IncidentChange{{At: detectNow().Add(-2 * time.Minute), Component: "db", Source: "app-state", ReaderOrObject: "s", Key: "k"}}})
	loop.Request.DetectedAt = detectNow().Add(-time.Minute)
	response := loop.Handle(context.Background(), ToolRequest{Command: "changes", Components: []string{"anything"}})
	if response.Error != "" {
		t.Fatalf("without a map there is nothing to validate against: %q", response.Error)
	}
	if response = loop.Handle(context.Background(), ToolRequest{Command: "changes", Components: []string{"db"}}); !strings.Contains(response.Text, ",db,") {
		t.Fatalf("the filter reads the rows' own Component:\n%s", response.Text)
	}
}

func TestTimelineComponentShowsTheOperationsThroughItAndItsWorkloadColumns(t *testing.T) {
	loop := componentLoop(t, &staticAppState{}, mapTestDoc)
	response := loop.Handle(context.Background(), ToolRequest{Command: "timeline", Components: []string{"jobs"}})
	header := strings.Split(strings.SplitN(response.Text, "\n", 2)[0], ",")
	if response.Error != "" || !containsString(header, "ready:jobs") || !containsString(header, "restarts:jobs") {
		t.Fatalf("jobs is off every failing path and still gets its columns: %v (%s)", header, response.Error)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "timeline", Components: []string{"cache"}})
	header = strings.Split(strings.SplitN(response.Text, "\n", 2)[0], ",")
	if !containsString(header, "fail:list") || containsString(header, "fail:create") {
		t.Fatalf("only the operations whose path contains cache: %v", header)
	}
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

// appStateWithReader is a real store holding one reader that has read once.
func appStateWithReader(id string, targets []podread.Target, snapshot carried.Snapshot) *AppStateStore {
	store := NewAppStateStore(time.Minute)
	store.Retain(map[string][]string{id: nil})
	store.SetTargets(id, targets)
	store.Record(id, snapshot)
	return store
}

func TestAppStateReaderRowsBindToTheComponentOfThePodsItReads(t *testing.T) {
	view := detectApp("create")
	m := program.AppMap{Components: []program.AppComponent{{Name: "db", Workload: "StatefulSet/db"}, {Name: "api", Workload: "Deployment/api"}, {Name: "front", Workload: "Deployment/front"}}}
	readers := []AppStateReaderStatus{
		{Reader: "limits", Targets: []podread.Target{{Labels: map[string]string{"app": "db"}}}},
		{Reader: "ambiguous", Targets: []podread.Target{{Workload: "Deployment/api"}, {Workload: "Deployment/front"}}},
		{Reader: "nobody", Targets: []podread.Target{{Labels: map[string]string{"app": "ghost"}}}},
		{Reader: "by-workload", Targets: []podread.Target{{Workload: "Deployment/api"}}},
	}
	got := readerComponents(view, m, "app", readers)
	if got["limits"] != "db" || got["by-workload"] != "api" {
		t.Fatalf("a reader's pod reads select Pods of one component: %v", got)
	}
	if _, ok := got["ambiguous"]; ok {
		t.Fatalf("Pods of two components leave the reader unresolved: %v", got)
	}
	if _, ok := got["nobody"]; ok {
		t.Fatalf("a target that selects no Pod leaves the reader unresolved: %v", got)
	}
}

func TestChangeRowsOfABoundReaderCarryItsComponent(t *testing.T) {
	now := detectNow()
	snapshot := carried.Snapshot{At: now.Add(-5 * time.Second), Error: "pod read unavailable"}
	store := appStateWithReader("limits", []podread.Target{{Labels: map[string]string{"app": "db"}}}, snapshot)
	statuses := store.AppStateReaders()
	if len(statuses) != 1 || statuses[0].Err == "" || len(statuses[0].Targets) != 1 || statuses[0].Baselined {
		t.Fatalf("statuses = %+v", statuses)
	}
	loop := componentLoop(t, store, mapTestDoc)
	response := loop.Handle(context.Background(), ToolRequest{Command: "changes", Components: []string{"db"}})
	if response.Error != "" || !strings.Contains(response.Text, "(read)") || !strings.Contains(response.Text, "pod read unavailable") {
		t.Fatalf("the failed reader's row is db's:\n%s (%s)", response.Text, response.Error)
	}
	// The same reader makes db's state a coverage gap in the detect view.
	text := loop.Handle(context.Background(), ToolRequest{Command: "detect"}).Text
	if !strings.Contains(text, "db [data] app-state reader limits unavailable: pod read unavailable") || !strings.Contains(text, "cache [data] no state reader") {
		t.Fatalf("detect:\n%s", text)
	}
}

func TestHealthyDiffRowsOfAReaderAreUntimedAndFirstSeenKeysCollapse(t *testing.T) {
	read := time.Date(2026, 11, 13, 7, 0, 0, 0, time.UTC)
	var rows []IncidentChange
	for _, key := range []string{"plan.a", "plan.b", "plan.c", "plan.d"} {
		rows = append(rows, IncidentChange{ReadAt: read, Source: "app-state", ReaderOrObject: "limits", Key: key, Before: AppStateAbsent, After: "10", FirstSeen: true})
	}
	rows = append(rows, IncidentChange{ReadAt: read, Source: "app-state", ReaderOrObject: "limits", Key: "rate", Before: "1", After: "2"})
	rows = collapseFirstSeen(rows)
	if len(rows) != 2 {
		t.Fatalf("four first-seen keys become one summary beside the real change: %+v", rows)
	}
	var summary IncidentChange
	for _, row := range rows {
		if row.FirstSeen {
			summary = row
		}
	}
	if !strings.Contains(summary.After, "4 keys, e.g. plan.a, plan.b, plan.c") || !summary.At.IsZero() {
		t.Fatalf("summary = %+v", summary)
	}
	evidence, _ := rowEvidence(summary, read)
	if evidence.Unexplained || !strings.Contains(evidence.Text, "not in the last healthy read") || !strings.Contains(evidence.Text, "read 07:00:00Z") {
		t.Fatalf("a first-seen row is weaker evidence and untimed: %+v", evidence)
	}
	// Two first-seen keys stay as rows: that is not noise.
	if got := collapseFirstSeen(rows[:1]); len(got) != 1 {
		t.Fatalf("%+v", got)
	}
}
