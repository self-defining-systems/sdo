package runtime

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

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
	timeline := buildIncidentTimeline(incidentPageInput{Request: request, View: detectApp("create"), Signals: signals, Now: now, Since: 3 * time.Minute})
	rows := parseCSV(t, timeline)
	if rows[0][0] != "t_rel_s" || rows[0][1] != "at_utc" {
		t.Fatalf("header = %v", rows[0])
	}
	// onset-180 s .. onset+40 s in 10 s buckets aligned on onset.
	if got := len(rows) - 1; got != 23 {
		t.Fatalf("%d buckets, want 23:\n%s", got, timeline)
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
	changes := buildIncidentChanges(incidentPageInput{Request: request, View: view, Signals: newIncidentSignals(), AppState: appState, Now: onset.Add(time.Minute), Since: 30 * time.Minute})
	rows := parseCSV(t, changes)
	if strings.Join(rows[0], ",") != "t_rel_s,at_utc,source,component,reader_or_object,key,before,after,writer,noisy" {
		t.Fatalf("header = %v", rows[0])
	}
	if len(rows) != 3 {
		t.Fatalf("rows:\n%s", changes)
	}
	state, departure := rows[1], rows[2]
	if strings.Join(state, ",") != "-60,"+onset.Add(-time.Minute).Format(time.RFC3339)+",app-state,db,settings,rate_limit,100,1,,true" {
		t.Fatalf("app-state row = %v", state)
	}
	if departure[0] != "-4" || departure[2] != "kubernetes" || departure[3] != "db" || departure[4] != "NetworkPolicy/restrict-db" || departure[5] != "spec.egress" || departure[8] != "kubectl" || departure[9] != "false" {
		t.Fatalf("departure row = %v", departure)
	}
	if !appState.since.Equal(onset.Add(-30 * time.Minute)) {
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
	changes := buildIncidentChanges(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: many}, Now: onset})
	rows := parseCSV(t, changes)
	if len(rows)-1 != incidentChangesCap {
		t.Fatalf("%d rows, want the cap %d", len(rows)-1, incidentChangesCap)
	}
	if rows[len(rows)-1][0] != "0" {
		t.Fatalf("the cap keeps the newest rows; last = %v", rows[len(rows)-1])
	}
	if strings.Contains(string(changes), "hunter2") {
		t.Fatal("a credential-shaped value reached sdo changes")
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

func onDemandLoop(t *testing.T, view detectFake, appState AppStateChangeSource) (*ResponderLoop, string) {
	t.Helper()
	request := detectRequest()
	loop := NewResponderLoop(request, nil, staticProvider{view.Snapshot}, nil, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	mount := t.TempDir()
	loop.CorpusSessionRoot = mount
	signals := newIncidentSignals()
	signals.Observe(detectNow(), view)
	loop.IncidentSources = func() (*incidentSignals, AppStateChangeSource) { return signals, appState }
	return loop, mount
}

func TestChangesAndTimelineCommandsPrintCSVAndWriteNoFiles(t *testing.T) {
	view := detectApp("create")
	onset := detectNow().Add(-time.Minute)
	appState := &staticAppState{rows: []IncidentChange{
		{At: onset.Add(-2 * time.Minute), Component: "db", ReaderOrObject: "settings", Key: "rate_limit", Before: "100", After: "1"},
		{At: onset.Add(-20 * time.Minute), Component: "db", ReaderOrObject: "settings", Key: "old", Before: "a", After: "b"},
	}}
	loop, mount := onDemandLoop(t, view, appState)
	loop.Request.DetectedAt = onset
	response := loop.Handle(context.Background(), ToolRequest{Command: "changes"})
	if response.Error != "" {
		t.Fatalf("changes error: %s", response.Error)
	}
	rows := parseCSV(t, []byte(response.Text))
	if strings.Join(rows[0], ",") != "t_rel_s,at_utc,source,component,reader_or_object,key,before,after,writer,noisy" {
		t.Fatalf("header = %v", rows[0])
	}
	if len(rows) != 2 || rows[1][5] != "rate_limit" {
		t.Fatalf("default window is 10 minutes before onset; rows:\n%s", response.Text)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "changes", Since: "30m"})
	if got := len(parseCSV(t, []byte(response.Text))); got != 3 {
		t.Fatalf("--since 30m lists %d rows:\n%s", got-1, response.Text)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "changes", Since: "30m", Limit: 1})
	if rows := parseCSV(t, []byte(response.Text)); len(rows) != 2 || rows[1][5] != "rate_limit" {
		t.Fatalf("--limit 1 keeps the newest row:\n%s", response.Text)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "timeline"})
	if response.Error != "" || !strings.HasPrefix(response.Text, "t_rel_s,at_utc,") {
		t.Fatalf("timeline = %q, error %q", response.Text, response.Error)
	}
	column(t, parseCSV(t, []byte(response.Text)), "fail:create")
	if response = loop.Handle(context.Background(), ToolRequest{Command: "changes", Since: "soon"}); response.Error == "" {
		t.Fatal("a malformed --since is refused")
	}
	entries, err := os.ReadDir(mount)
	if err != nil || len(entries) != 0 {
		t.Fatalf("nothing is written into the session mount: %v, %v", entries, err)
	}
}

func TestDetectShowsRecentChangesInlineNewestFirst(t *testing.T) {
	view := detectApp("create")
	onset := detectNow().Add(-time.Minute)
	var rows []IncidentChange
	for index := range 12 {
		rows = append(rows, IncidentChange{At: onset.Add(-time.Duration(index+1) * 30 * time.Second), Component: "db", ReaderOrObject: "settings", Key: fmt.Sprintf("k%d", index), Before: "1", After: "2", Writer: "churn"})
	}
	loop, _ := onDemandLoop(t, view, &staticAppState{rows: rows})
	loop.Request.DetectedAt = onset
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	if strings.Contains(response.Text, "files:") {
		t.Fatalf("detect still names files:\n%s", response.Text)
	}
	if !strings.Contains(response.Text, "-30s app-state db settings k0: 1 → 2 by: churn") || strings.Contains(response.Text, " k10:") {
		t.Fatalf("inline changes:\n%s", response.Text)
	}
	if strings.Index(response.Text, " k0:") > strings.Index(response.Text, " k1:") || !strings.Contains(response.Text, incidentPointer) {
		t.Fatalf("newest first, then the pointer:\n%s", response.Text)
	}
}

func TestRecentChangeLinesAreCappedAndMaskCredentials(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{
		{Component: "db", ReaderOrObject: "ConfigMap/lost", After: "missing"},
		{At: onset.Add(-125 * time.Second), Source: "app-state", Component: "cache", ReaderOrObject: "config", Key: "requirepass", Before: "old", After: "hunter2"},
		{At: onset.Add(5 * time.Second), Source: "kubernetes", Component: "api", ReaderOrObject: "Deployment/api", Key: "spec.image", Before: "accepted-source", After: "drifted", Writer: "kubectl-patch"},
	}
	lines := recentChangeLines(rows, onset)
	want := []string{
		"+5s kubernetes api Deployment/api spec.image: accepted-source → drifted by: kubectl-patch",
		"-2m05s app-state cache config requirepass: [redacted] → [redacted]",
		"? db ConfigMap/lost: (none) → missing",
		incidentPointer,
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("lines:\n%s", strings.Join(lines, "\n"))
	}
	var many []IncidentChange
	for index := range 30 {
		many = append(many, IncidentChange{At: onset.Add(-time.Duration(index) * time.Second), Component: "db"})
	}
	if got := len(recentChangeLines(many, onset)); got != incidentInlineChanges+1 {
		t.Fatalf("%d lines, want %d", got, incidentInlineChanges+1)
	}
}

// The controller carries the inline changes in the incident request, so the
// first message shows them without any file.
func TestDispatchCarriesRecentChanges(t *testing.T) {
	fault := classedDetector("workload-health", sdk.DetectorClassHealth, true)
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	config := testControllerConfig()
	config.AppStateChanges = &staticAppState{rows: []IncidentChange{{At: time.Unix(0, 0).UTC(), Component: "db", ReaderOrObject: "settings", Key: "rate_limit", Before: "100", After: "1"}}}
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
	last := len(request.RecentChanges) - 1
	if last < 1 || request.RecentChanges[last] != incidentPointer || !strings.Contains(strings.Join(request.RecentChanges, "\n"), "settings rate_limit: 100 → 1") {
		t.Fatalf("recent changes = %q", request.RecentChanges)
	}
}

// A value is masked when its key (or the row's object) names a credential,
// even though the value itself carries no credential shape.
func TestIncidentChangesMaskValuesOfCredentialNamedKeys(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{{At: onset, Component: "cache", ReaderOrObject: "config", Key: "requirepass", Before: "old-pass", After: "hunter2"}}
	changes := buildIncidentChanges(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: rows}, Now: onset})
	if text := string(changes); strings.Contains(text, "hunter2") || strings.Contains(text, "old-pass") || !strings.Contains(text, "requirepass") {
		t.Fatalf("sdo changes:\n%s", text)
	}
}

// The cap drops the oldest timed rows, never an untimed (missing object) row.
func TestIncidentChangesCapKeepsUntimedRows(t *testing.T) {
	onset := detectOnset
	rows := []IncidentChange{{Component: "db", ReaderOrObject: "ConfigMap/lost", After: "missing"}}
	for index := range incidentChangesCap + 5 {
		rows = append(rows, IncidentChange{At: onset.Add(-time.Duration(index) * time.Second), Component: "db", Key: fmt.Sprintf("k%d", index)})
	}
	changes := buildIncidentChanges(incidentPageInput{Request: IncidentRequest{IncidentID: "i", DetectedAt: onset}, View: detectApp(), Signals: newIncidentSignals(), AppState: &staticAppState{rows: rows}, Now: onset})
	parsed := parseCSV(t, changes)
	if len(parsed)-1 != incidentChangesCap || !strings.Contains(string(changes), "ConfigMap/lost") {
		t.Fatalf("%d rows; untimed row kept: %v", len(parsed)-1, strings.Contains(string(changes), "ConfigMap/lost"))
	}
}
