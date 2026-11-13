package runtime

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
	"sdo.dev/operator/sdk/traffic"
)

func parseCSV(t *testing.T, data []byte) [][]string {
	t.Helper()
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil {
		t.Fatalf("csv: %v\n%s", err, data)
	}
	return rows
}

func column(t *testing.T, rows [][]string, name string) int {
	t.Helper()
	for index, header := range rows[0] {
		if header == name {
			return index
		}
	}
	t.Fatalf("no column %q in %v", name, rows[0])
	return -1
}

// pageSnapshot is the detect fixture with one scenario sample at at, failing or
// not, and the api Deployment at ready/desired with restarts on its Pod.
func pageSnapshot(at time.Time, failed bool, ready, desired, restarts int32) detectFake {
	view := detectApp()
	sample := traffic.Sample{At: at, Outcome: traffic.OutcomeOK}
	if failed {
		sample.Outcome, sample.Status = traffic.OutcomeError, 500
	}
	view.Traffic = map[string]traffic.Window{"health": {ObservedAt: at, Scenarios: []traffic.ScenarioObservations{
		{Scenario: traffic.Descriptor{ID: "create", Target: traffic.Target{Service: "front"}, DependsOn: []string{"api"}}, Samples: []traffic.Sample{sample}},
	}}}
	deployments := append([]appsv1.Deployment(nil), view.DeploymentList...)
	for index := range deployments {
		if deployments[index].Name == "api" {
			deployments[index].Spec.Replicas = &desired
			deployments[index].Status.ReadyReplicas = ready
		}
	}
	view.DeploymentList = deployments
	pods := append([]corev1.Pod(nil), view.PodList...)
	for index := range pods {
		if pods[index].Name == "api-0" {
			pods[index].Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", RestartCount: restarts}}
		}
	}
	view.PodList = pods
	return view
}

func TestIncidentSignalsBucketFailRatiosAndWorkloadsOnOnset(t *testing.T) {
	signals := newIncidentSignals()
	onset := detectOnset
	// Healthy before onset, failing after; api loses a Pod and restarts at onset+15s.
	for second := -200; second <= 40; second += 5 {
		at := onset.Add(time.Duration(second) * time.Second)
		ready, restarts := int32(2), int32(0)
		if second >= 15 {
			ready, restarts = 1, 3
		}
		view := pageSnapshot(at, second >= 0, ready, 2, restarts)
		signals.Observe(at, view)
		// A repeated evaluation of the same window adds no sample twice.
		signals.Observe(at, view)
	}
	now := onset.Add(40 * time.Second)
	request := detectRequest()
	request.DetectedAt = onset
	pages := buildIncidentPages(incidentPageInput{Request: request, View: detectApp("create"), Signals: signals, Now: now})
	rows := parseCSV(t, pages.Timeline)
	if rows[0][0] != "t_rel_s" || rows[0][1] != "at_utc" {
		t.Fatalf("header = %v", rows[0])
	}
	// onset-180 s .. onset+40 s in 10 s buckets aligned on onset.
	if got := len(rows) - 1; got != 23 {
		t.Fatalf("%d buckets, want 23:\n%s", got, pages.Timeline)
	}
	if rows[1][0] != "-180" || rows[len(rows)-1][0] != "40" {
		t.Fatalf("buckets %s..%s", rows[1][0], rows[len(rows)-1][0])
	}
	fail, ready, desired, restarts := column(t, rows, "fail:create"), column(t, rows, "ready:api"), column(t, rows, "desired:api"), column(t, rows, "restarts:api")
	byRel := map[string][]string{}
	for _, row := range rows[1:] {
		byRel[row[0]] = row
	}
	if got := byRel["-10"][fail]; got != "0.00" {
		t.Fatalf("fail ratio before onset = %q", got)
	}
	if got := byRel["0"][fail]; got != "1.00" {
		t.Fatalf("fail ratio at onset = %q", got)
	}
	if byRel["0"][ready] != "2" || byRel["10"][ready] != "1" || byRel["10"][desired] != "2" || byRel["10"][restarts] != "3" {
		t.Fatalf("workload signals: %v / %v", byRel["0"], byRel["10"])
	}
}

func TestIncidentSignalsKeepFifteenMinutes(t *testing.T) {
	signals := newIncidentSignals()
	start := detectOnset.Add(-time.Hour)
	for minute := 0; minute <= 60; minute++ {
		at := start.Add(time.Duration(minute) * time.Minute)
		signals.Observe(at, pageSnapshot(at, false, 2, 2, 0))
	}
	history := signals.history()
	for _, samples := range history.operations {
		if first := samples[0].at; first.Before(detectOnset.Add(-incidentSignalHorizon)) {
			t.Fatalf("operation sample at %s outlived the ring", first)
		}
	}
}

func TestIncidentChangesListDeparturesWithWriterInTheWindow(t *testing.T) {
	onset := detectOnset
	view := departureView{detectFake: detectApp(), diffs: map[string]sdk.SourceDiff{
		"NetworkPolicy/restrict-db": {Changed: true},
		"Service/api":               {Changed: true},
	}}
	request := detectRequest()
	request.DetectedAt = onset
	request.DriftEvents = []sdk.DriftEvent{
		departureEvent(sdk.ObjectRef{Kind: "NetworkPolicy", Namespace: "app", Name: "restrict-db"}, "kubectl", onset.Add(-4*time.Second), "spec.egress"),
		// Older than the 30-minute window: not listed.
		departureEvent(dref("Service", "api"), "helm", onset.Add(-31*time.Minute), "spec.ports"),
	}
	appState := &staticAppState{rows: []IncidentChange{{At: onset.Add(-time.Minute), Component: "db", ReaderOrObject: "settings", Key: "rate_limit", Before: "100", After: "1", Noisy: true}}}
	pages := buildIncidentPages(incidentPageInput{Request: request, View: view, Signals: newIncidentSignals(), AppState: appState, Now: onset.Add(time.Minute)})
	rows := parseCSV(t, pages.Changes)
	if strings.Join(rows[0], ",") != "t_rel_s,at_utc,source,component,reader_or_object,key,before,after,writer,noisy" {
		t.Fatalf("header = %v", rows[0])
	}
	if len(rows) != 3 {
		t.Fatalf("rows:\n%s", pages.Changes)
	}
	state, departure := rows[1], rows[2]
	if strings.Join(state, ",") != "-60,"+onset.Add(-time.Minute).Format(time.RFC3339)+",app-state,db,settings,rate_limit,100,1,,true" {
		t.Fatalf("app-state row = %v", state)
	}
	if departure[0] != "-4" || departure[2] != "kubernetes" || departure[3] != "db" || departure[4] != "NetworkPolicy/restrict-db" || departure[5] != "spec.egress" || departure[8] != "kubectl" || departure[9] != "false" {
		t.Fatalf("departure row = %v", departure)
	}
	if !appState.since.Equal(onset.Add(-incidentChangesWindow)) {
		t.Fatalf("app-state source asked from %s", appState.since)
	}
}

type staticAppState struct {
	rows  []IncidentChange
	since time.Time
}

func (s *staticAppState) AppStateChanges(since, now time.Time) []IncidentChange {
	s.since = since
	return s.rows
}

func TestIncidentChangesNeverCarrySecretValuesAndAreCapped(t *testing.T) {
	onset := detectOnset
	var many []IncidentChange
	for index := range incidentChangesCap + 50 {
		many = append(many, IncidentChange{At: onset.Add(-time.Duration(index) * time.Second), Component: "db", ReaderOrObject: "settings", Key: fmt.Sprintf("k%d", index), Before: "password=hunter2", After: "x"})
	}
	pages := buildIncidentPages(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: many}, Now: onset})
	rows := parseCSV(t, pages.Changes)
	if len(rows)-1 != incidentChangesCap {
		t.Fatalf("%d rows, want the cap %d", len(rows)-1, incidentChangesCap)
	}
	if rows[len(rows)-1][0] != "0" {
		t.Fatalf("the cap keeps the newest rows; last = %v", rows[len(rows)-1])
	}
	if strings.Contains(string(pages.Changes), "hunter2") {
		t.Fatal("a credential-shaped value reached changes.csv")
	}
}

func TestStoreChangeRowsUseTheObservedTimeAndBoundedValues(t *testing.T) {
	observed := detectOnset.Add(-time.Minute)
	stores := []storestate.StoreChanges{{
		Component: "db", Engine: "postgres", Pod: sdk.ObjectRef{Kind: "Pod", Namespace: "app", Name: "db-0"}, ObservedAt: observed, Baselined: true,
		Changes: []storestate.Change{{Before: &storestate.Item{Kind: "setting", Name: "max_connections", Value: "100"}, After: &storestate.Item{Kind: "setting", Name: "max_connections", Value: "5"}}},
	}, {Component: "cache", Error: "no readings"}}
	rows := storeChangeRows(stores)
	if len(rows) != 1 || rows[0].Source != "store" || rows[0].Component != "db" || rows[0].ReaderOrObject != "Pod/db-0" || rows[0].Before != "100" || rows[0].After != "5" || !rows[0].At.Equal(observed) {
		t.Fatalf("store rows = %+v", rows)
	}
}

func TestDetectWritesIncidentPagesAndNamesThem(t *testing.T) {
	view := detectApp("create")
	request := detectRequest()
	loop := NewResponderLoop(request, nil, staticProvider{view.Snapshot}, nil, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	mount := t.TempDir()
	signals := newIncidentSignals()
	signals.Observe(detectNow(), view)
	if _, err := memory.PrepareIncidentSession(mount, "", request.IncidentID); err != nil {
		t.Fatal(err)
	}
	controller := &Controller{signals: signals, config: ControllerConfig{PageSessionRoot: mount}}
	loop.IncidentPages = controller.IncidentPages(context.Background())
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if response.Error != "" || !strings.Contains(response.Text, "files: /sdo/session/incident/timeline.csv /sdo/session/incident/changes.csv") {
		t.Fatalf("detect text = %q, error %q", response.Text, response.Error)
	}
	sub, err := memory.IncidentSessionSubPath("", request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"timeline.csv", "changes.csv"} {
		if _, err := os.Stat(filepath.Join(mount, sub, memory.IncidentPagesDir, name)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// The controller writes both pages into the responder's session when it opens
// the incident, before any `sdo detect`.
func TestDispatchWritesIncidentPages(t *testing.T) {
	fault := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.PageSessionRoot = t.TempDir()
	controller, err := NewController(config, []sdk.Detector{fault}, staticProvider{snapshot: detectApp("create")}, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.now = func() time.Time { return time.Unix(1, 0).UTC() }
	for sample := 0; sample < 2; sample++ {
		if err := controller.Step(context.Background(), time.Unix(int64(sample), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	sub, err := memory.IncidentSessionSubPath("", request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range incidentPageNames {
		data, err := os.ReadFile(filepath.Join(config.PageSessionRoot, sub, memory.IncidentPagesDir, name))
		if err != nil || !strings.HasPrefix(string(data), "t_rel_s,at_utc,") {
			t.Fatalf("%s = %q, %v", name, data, err)
		}
	}
	timeline := parseCSV(t, mustRead(t, filepath.Join(config.PageSessionRoot, sub, memory.IncidentPagesDir, "timeline.csv")))
	column(t, timeline, "fail:create")
}

func mustRead(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// A value is masked when its key (or the row's object) names a credential,
// even though the value itself carries no credential shape.
func TestIncidentChangesMaskValuesOfCredentialNamedKeys(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{{At: onset, Component: "cache", ReaderOrObject: "config", Key: "requirepass", Before: "old-pass", After: "hunter2"}}
	pages := buildIncidentPages(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: rows}, Now: onset})
	if text := string(pages.Changes); strings.Contains(text, "hunter2") || strings.Contains(text, "old-pass") || !strings.Contains(text, "requirepass") {
		t.Fatalf("changes.csv:\n%s", text)
	}
}

// The cap drops the oldest timed rows, never an untimed (missing object) row.
func TestIncidentChangesCapKeepsUntimedRows(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{{Component: "db", ReaderOrObject: "ConfigMap/lost", After: "missing"}}
	for index := range incidentChangesCap + 5 {
		rows = append(rows, IncidentChange{At: onset.Add(-time.Duration(index) * time.Second), Component: "db", Key: fmt.Sprintf("k%d", index)})
	}
	pages := buildIncidentPages(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: rows}, Now: onset})
	parsed := parseCSV(t, pages.Changes)
	if len(parsed)-1 != incidentChangesCap || !strings.Contains(string(pages.Changes), "ConfigMap/lost") {
		t.Fatalf("%d rows; untimed row kept: %v", len(parsed)-1, strings.Contains(string(pages.Changes), "ConfigMap/lost"))
	}
}
