package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	"sdo.dev/operator/runtime/prometheus"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

var metricsNow = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

// fakePrometheus serves canned answers by expression and records every request.
type fakePrometheus struct {
	mu       sync.Mutex
	requests []*http.Request
	queries  map[string]string // expression -> JSON body
	alerts   string
	status   int
	hang     map[string]bool
	release  chan struct{}
}

func newFakePrometheus(t *testing.T) (*fakePrometheus, *httptest.Server) {
	t.Helper()
	fake := &fakePrometheus{queries: map[string]string{}, hang: map[string]bool{}, release: make(chan struct{})}
	server := httptest.NewServer(fake)
	t.Cleanup(func() {
		close(fake.release)
		server.Close()
	})
	return fake, server
}

func (f *fakePrometheus) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests = append(f.requests, r.Clone(context.Background()))
	status, alerts := f.status, f.alerts
	expression := r.URL.Query().Get("query")
	body, known := f.queries[expression]
	hang := f.hang[expression]
	f.mu.Unlock()
	if hang {
		select {
		case <-f.release:
		case <-r.Context().Done():
		}
		return
	}
	if status != 0 {
		http.Error(w, "backend exploded", status)
		return
	}
	switch r.URL.Path {
	case "/api/v1/alerts":
		_, _ = w.Write([]byte(alerts))
	case "/api/v1/query", "/api/v1/query_range":
		if !known {
			http.Error(w, `{"status":"error","errorType":"bad_data","error":"unknown expression"}`, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(body))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePrometheus) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	paths := make([]string, 0, len(f.requests))
	for _, request := range f.requests {
		paths = append(paths, request.URL.Path)
	}
	return paths
}

func vector(results ...string) string {
	return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(results, ",") + `]}}`
}

func metricsDetector(id string, queries ...metrics.Query) sdk.Detector {
	rules := make([]metrics.ThresholdRule, 0, len(queries))
	for _, query := range queries {
		rules = append(rules, metrics.ThresholdRule{Query: query, Op: metrics.Greater, Threshold: 0.05})
	}
	return metrics.NewThresholdDetector(metricsSpec(id), rules)
}

// alertReader is the smallest detector that consumes alerts, so the observer
// tests do not depend on a policy detector.
type alertReader struct{ spec sdk.DetectorSpec }

func (a alertReader) Spec() sdk.DetectorSpec { return a.spec }

func (alertReader) ConsumesAlerts() bool { return true }

func (alertReader) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	frozen, ok := metrics.AlertsFrom(ctx)
	if !ok {
		return nil, errors.New("alerts unavailable: detection context carries no alerts")
	}
	if frozen.Error != "" {
		return nil, fmt.Errorf("alerts unavailable: %s", frozen.Error)
	}
	return nil, nil
}

func metricsSpec(id string) sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{metrics.Watch}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

var (
	errorQuery = metrics.Query{ID: "error-rate", Expr: `sum by (deployment) (rate(errors[1m]))`}
	queueQuery = metrics.Query{ID: "queue", Expr: `queue_depth`, Range: 5 * time.Minute, Step: time.Minute}
)

func snapshotWith(t *testing.T, api MetricsAPI, detectors []sdk.Detector, options MetricsObserverOptions) (sdk.DetectionContext, *MetricsObserver) {
	t.Helper()
	observer, err := NewMetricsObserver(api, detectors, options)
	if err != nil {
		t.Fatal(err)
	}
	provider := MetricsSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop"}}, Observer: observer}
	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, observer
}

func fixedClock() func() time.Time { return func() time.Time { return metricsNow } }

func TestMetricsObserverFreezesInstantRangeAndAlertResults(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"web"},"value":[1790856000,"0.4"]}`)
	fake.queries[queueQuery.Expr] = `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"service":"q"},"values":[[1790855940,"1"],[1790856000,"3"]]}]}}`
	fake.alerts = `{"status":"success","data":{"alerts":[{"labels":{"alertname":"A","namespace":"shop"},"state":"firing","activeAt":"2026-10-01T11:00:00Z"}]}}`
	detectors := []sdk.Detector{
		metricsDetector("thresholds", errorQuery, queueQuery),
		alertReader{spec: metricsSpec("alerts")},
	}
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, detectors, MetricsObserverOptions{Clock: fixedClock()})

	instant, ok := metrics.ResultFrom(snapshot, "error-rate")
	if !ok || instant.Error != "" || !instant.ObservedAt.Equal(metricsNow) || len(instant.Series) != 1 || instant.Series[0].Samples[0].Value != 0.4 {
		t.Fatalf("instant result: %+v", instant)
	}
	ranged, ok := metrics.ResultFrom(snapshot, "queue")
	if !ok || ranged.Error != "" || len(ranged.Series) != 1 || len(ranged.Series[0].Samples) != 2 {
		t.Fatalf("range result: %+v", ranged)
	}
	alerts, ok := metrics.AlertsFrom(snapshot)
	if !ok || alerts.Error != "" || len(alerts.Alerts) != 1 || alerts.Alerts[0].Name() != "A" || !alerts.ObservedAt.Equal(metricsNow) {
		t.Fatalf("alerts: %+v", alerts)
	}
	paths := strings.Join(fake.paths(), ",")
	for _, want := range []string{"/api/v1/query", "/api/v1/query_range", "/api/v1/alerts"} {
		if !strings.Contains(paths, want) {
			t.Errorf("missing request to %s: %s", want, paths)
		}
	}
	for _, detector := range detectors {
		if _, err := detector.Detect(context.Background(), snapshot); err != nil {
			t.Fatalf("%s: %v", detector.Spec().ID, err)
		}
	}
}

func TestMetricsObserverEvaluatesEachQueryOncePerCycleAtOneTimestamp(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"web"},"value":[1790856000,"0.4"]}`)
	detectors := []sdk.Detector{metricsDetector("a", errorQuery), metricsDetector("b", errorQuery)}
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, detectors, MetricsObserverOptions{Clock: fixedClock()})
	for round := 0; round < 3; round++ {
		for _, detector := range detectors {
			findings, err := detector.Detect(context.Background(), snapshot)
			if err != nil || len(findings) != 1 {
				t.Fatalf("round %d %s: %+v %v", round, detector.Spec().ID, findings, err)
			}
		}
	}
	if len(fake.requests) != 1 {
		t.Fatalf("a shared query must be evaluated once per cycle, got %d requests", len(fake.requests))
	}
	if got := fake.requests[0].URL.Query().Get("time"); got != "1790856000" {
		t.Fatalf("query time %q", got)
	}
}

func TestMetricsObserverDoesNotQueryUntilADetectorReadsAndRefreshesEachCycle(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector()
	times := []time.Time{metricsNow, metricsNow.Add(30 * time.Second)}
	clock := func() time.Time {
		next := times[0]
		if len(times) > 1 {
			times = times[1:]
		}
		return next
	}
	detectors := []sdk.Detector{metricsDetector("a", errorQuery)}
	observer, err := NewMetricsObserver(prometheus.Client{BaseURL: server.URL}, detectors, MetricsObserverOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	provider := MetricsSnapshotProvider{Base: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop"}}, Observer: observer}
	first, _ := provider.Snapshot(context.Background())
	second, _ := provider.Snapshot(context.Background())
	if len(fake.requests) != 0 {
		t.Fatalf("a cycle whose detectors do not read metrics must not query: %d requests", len(fake.requests))
	}
	for _, snapshot := range []sdk.DetectionContext{first, second} {
		if _, err := detectors[0].Detect(context.Background(), snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if len(fake.requests) != 2 || fake.requests[0].URL.Query().Get("time") == fake.requests[1].URL.Query().Get("time") {
		t.Fatalf("each cycle must query once at its own timestamp: %d requests", len(fake.requests))
	}
}

func TestMetricsObserverFetchesOnlyDeclaredInputs(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector()
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, []sdk.Detector{metricsDetector("a", errorQuery)}, MetricsObserverOptions{Clock: fixedClock()})
	if _, ok := metrics.AlertsFrom(snapshot); ok {
		t.Fatal("alerts were not declared and must not be fetched")
	}
	if _, ok := metrics.ResultFrom(snapshot, "undeclared"); ok {
		t.Fatal("undeclared queries have no result")
	}
	for _, path := range fake.paths() {
		if path == "/api/v1/alerts" {
			t.Fatal("alerts endpoint was called")
		}
	}
}

func TestMetricsObserverUnavailableSourceIsADetectorErrorNeverClear(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector()
	cases := map[string]func(){
		"5xx":            func() { fake.status = http.StatusBadGateway },
		"malformed json": func() { fake.queries[errorQuery.Expr] = `{"status": "succ`; fake.alerts = `not json` },
		"timeout":        func() { fake.hang[errorQuery.Expr] = true },
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			arrange()
			t.Cleanup(func() { fake.status, fake.hang = 0, map[string]bool{}; fake.queries[errorQuery.Expr] = vector() })
			detectors := []sdk.Detector{
				metricsDetector("thresholds", errorQuery),
				alertReader{spec: metricsSpec("alerts")},
			}
			snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, detectors, MetricsObserverOptions{Clock: fixedClock(), QueryTimeout: 200 * time.Millisecond})
			for _, detector := range detectors {
				findings, err := detector.Detect(context.Background(), snapshot)
				if err == nil || len(findings) != 0 || !strings.Contains(err.Error(), "unavailable") {
					t.Errorf("%s: unavailable metrics must be a detector error, got %+v %v", detector.Spec().ID, findings, err)
				}
			}
		})
	}
}

func TestMetricsObserverTimeoutBoundsTheCycleAndSparesOtherQueries(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.hang[errorQuery.Expr] = true
	fake.queries[`queue_depth`] = vector(`{"metric":{},"value":[1790856000,"1"]}`)
	other := metrics.Query{ID: "queue", Expr: `queue_depth`}
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL},
		[]sdk.Detector{metricsDetector("a", errorQuery, other)}, MetricsObserverOptions{Clock: fixedClock(), QueryTimeout: 150 * time.Millisecond})
	started := time.Now()
	slow, _ := metrics.ResultFrom(snapshot, "error-rate")
	if time.Since(started) > 3*time.Second {
		t.Fatalf("the per-query timeout did not bound the cycle: %s", time.Since(started))
	}
	if slow.Error == "" || len(slow.Series) != 0 {
		t.Fatalf("a timed out query is unavailable: %+v", slow)
	}
	fast, _ := metrics.ResultFrom(snapshot, "queue")
	if fast.Error != "" || len(fast.Series) != 1 {
		t.Fatalf("one slow query must not poison the others: %+v", fast)
	}
}

type ignoringContextAPI struct{ release chan struct{} }

func (a ignoringContextAPI) Query(context.Context, metrics.Query, time.Time) ([]metrics.Series, error) {
	<-a.release
	return nil, nil
}

func (a ignoringContextAPI) Alerts(context.Context) ([]metrics.Alert, error) {
	<-a.release
	return nil, nil
}

func TestMetricsObserverTimeoutHoldsEvenWhenTheSourceIgnoresItsContext(t *testing.T) {
	api := ignoringContextAPI{release: make(chan struct{})}
	t.Cleanup(func() { close(api.release) })
	snapshot, _ := snapshotWith(t, api, []sdk.Detector{metricsDetector("a", errorQuery)}, MetricsObserverOptions{QueryTimeout: 100 * time.Millisecond})
	started := time.Now()
	result, _ := metrics.ResultFrom(snapshot, "error-rate")
	if time.Since(started) > 3*time.Second || result.Error == "" {
		t.Fatalf("result %+v after %s", result, time.Since(started))
	}
}

func TestMetricsObserverWithoutASourceIsUnavailableNotClear(t *testing.T) {
	detector := metricsDetector("a", errorQuery)
	snapshot, _ := snapshotWith(t, nil, []sdk.Detector{detector, alertReader{spec: metricsSpec("alerts")}}, MetricsObserverOptions{})
	if findings, err := detector.Detect(context.Background(), snapshot); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("an unconfigured source must be an error, got %+v %v", findings, err)
	}
	alerts, _ := metrics.AlertsFrom(snapshot)
	if alerts.Error == "" {
		t.Fatalf("unconfigured alerts must carry an error: %+v", alerts)
	}
}

func TestMetricsObserverRejectsOversizedResults(t *testing.T) {
	fake, server := newFakePrometheus(t)
	items := make([]string, metrics.MaxSeries+1)
	for index := range items {
		items[index] = `{"metric":{"i":"` + strings.Repeat("x", index%7) + string(rune('a'+index%26)) + string(rune('a'+index/26%26)) + string(rune('a'+index/676)) + `"},"value":[1790856000,"1"]}`
	}
	fake.queries[errorQuery.Expr] = vector(items...)
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, []sdk.Detector{metricsDetector("a", errorQuery)}, MetricsObserverOptions{Clock: fixedClock()})
	result, _ := metrics.ResultFrom(snapshot, "error-rate")
	if result.Error == "" || !strings.Contains(result.Error, "series") || len(result.Series) != 0 {
		t.Fatalf("a result above the bound must be unavailable, not truncated: %d series, error %q", len(result.Series), result.Error)
	}
}

func TestMetricsObserverRejectsTooManyAlerts(t *testing.T) {
	fake, server := newFakePrometheus(t)
	alerts := make([]string, metrics.MaxAlerts+1)
	for index := range alerts {
		alerts[index] = `{"labels":{"alertname":"A","namespace":"shop"},"state":"pending"}`
	}
	fake.alerts = `{"status":"success","data":{"alerts":[` + strings.Join(alerts, ",") + `]}}`
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, []sdk.Detector{alertReader{spec: metricsSpec("alerts")}}, MetricsObserverOptions{})
	frozen, _ := metrics.AlertsFrom(snapshot)
	if frozen.Error == "" || len(frozen.Alerts) != 0 {
		t.Fatalf("too many alerts must be unavailable, not truncated: %d alerts, error %q", len(frozen.Alerts), frozen.Error)
	}
}

func TestMetricsResultsAreSortedForDeterminism(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(
		`{"metric":{"deployment":"b"},"value":[1790856000,"1"]}`,
		`{"metric":{"deployment":"a"},"value":[1790856000,"1"]}`,
	)
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, []sdk.Detector{metricsDetector("a", errorQuery)}, MetricsObserverOptions{Clock: fixedClock()})
	result, _ := metrics.ResultFrom(snapshot, "error-rate")
	if len(result.Series) != 2 || result.Series[0].Labels["deployment"] != "a" {
		t.Fatalf("series must be sorted by labels: %+v", result.Series)
	}
}

func TestNewMetricsObserverValidatesDeclaredInputs(t *testing.T) {
	if observer, err := NewMetricsObserver(nil, []sdk.Detector{trafficDetector("t", "health")}, MetricsObserverOptions{}); observer != nil || err != nil {
		t.Fatalf("no metrics consumer needs no observer: %v %v", observer, err)
	}
	conflicting := metrics.Query{ID: "error-rate", Expr: "different"}
	if _, err := NewMetricsObserver(nil, []sdk.Detector{metricsDetector("a", errorQuery), metricsDetector("b", conflicting)}, MetricsObserverOptions{}); err == nil {
		t.Fatal("conflicting query definitions must be rejected")
	}
	invalid := metrics.NewThresholdDetector(metricsSpec("bad"), []metrics.ThresholdRule{{Query: metrics.Query{ID: "x"}, Op: metrics.Greater}})
	if _, err := NewMetricsObserver(nil, []sdk.Detector{invalid}, MetricsObserverOptions{}); err == nil {
		t.Fatal("a misconfigured detector must be rejected at startup")
	}
	many := make([]sdk.Detector, 0, metrics.MaxQueries+1)
	for index := 0; index <= metrics.MaxQueries; index++ {
		query := metrics.Query{ID: string(rune('a'+index%26)) + string(rune('a'+index/26)), Expr: "up"}
		many = append(many, metricsDetector("d"+query.ID, query))
	}
	if _, err := NewMetricsObserver(nil, many, MetricsObserverOptions{}); err == nil {
		t.Fatal("more than the bounded number of queries must be rejected")
	}
}

func TestMetricsSnapshotProviderKeepsTrafficAndOptionalViews(t *testing.T) {
	base := sdktest.Snapshot{
		NamespaceName:   "shop",
		StatefulSetList: []appsv1.StatefulSet{{}},
		Traffic:         map[string]traffic.Window{"health": {LoadError: "boom"}},
	}
	observer, err := NewMetricsObserver(nil, []sdk.Detector{metricsDetector("a", errorQuery)}, MetricsObserverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := MetricsSnapshotProvider{Base: staticProvider{snapshot: base}, Observer: observer}.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if window, ok := traffic.WindowFrom(snapshot, "health"); !ok || window.LoadError != "boom" {
		t.Fatalf("traffic windows must survive wrapping: %+v %v", window, ok)
	}
	extended, ok := snapshot.(interface{ StatefulSets() []appsv1.StatefulSet })
	if !ok || len(extended.StatefulSets()) != 1 {
		t.Fatal("optional workload views must survive wrapping")
	}
	if snapshot.Namespace() != "shop" {
		t.Fatalf("namespace: %s", snapshot.Namespace())
	}
}

func TestMetricsSnapshotProviderWithoutObserverIsTransparent(t *testing.T) {
	base := staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop"}}
	snapshot, err := MetricsSnapshotProvider{Base: base}.Snapshot(context.Background())
	if err != nil || snapshot.Namespace() != "shop" {
		t.Fatalf("snapshot %v err %v", snapshot, err)
	}
	if _, ok := snapshot.(metrics.Source); !ok {
		// sdktest snapshots are sources; the wrapper must not hide that either.
		t.Fatal("snapshot must remain a metrics source")
	}
}

func evaluateOnceWithMetrics(t *testing.T, detectors []sdk.Detector, args ...string) (string, error) {
	t.Helper()
	var stdout bytes.Buffer
	err := RunWithOptions(context.Background(), detectors, RuntimeOptions{
		Args:     append([]string{"--evaluate-once", "--namespace", "shop"}, args...),
		Stdout:   &stdout,
		Provider: staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "shop"}},
	})
	return stdout.String(), err
}

func TestEvaluateOnceWithMetricsURLReportsAFiringThreshold(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"checkout","namespace":"shop"},"value":[1790856000,"0.4"]}`)
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var evaluation struct {
		Status   string        `json:"status"`
		Findings []sdk.Finding `json:"findings"`
	}
	if err := json.Unmarshal([]byte(output), &evaluation); err != nil {
		t.Fatalf("decode %q: %v", output, err)
	}
	if evaluation.Status != "firing" || len(evaluation.Findings) != 1 || evaluation.Findings[0].PrimaryResource.Name != "checkout" ||
		evaluation.Findings[0].Metadata["query_id"] != "error-rate" {
		t.Fatalf("evaluation: %s", output)
	}
}

func TestEvaluateOnceWithMetricsURLReportsClearOnlyWhenTheQueryAnswered(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries[errorQuery.Expr] = vector(`{"metric":{"deployment":"checkout"},"value":[1790856000,"0.01"]}`)
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err != nil || !strings.Contains(output, `"status":"clear"`) {
		t.Fatalf("an answered healthy query is clear: %q %v", output, err)
	}
}

func TestEvaluateOnceFailsClosedWhenTheMetricsSourceIsUnavailable(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.status = http.StatusServiceUnavailable
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", server.URL)
	if err == nil || !strings.Contains(err.Error(), "unavailable") || strings.Contains(output, `"clear"`) {
		t.Fatalf("an unavailable source must fail evaluation, not clear it: %q %v", output, err)
	}
	if _, err := evaluateOnceWithMetrics(t, []sdk.Detector{alertReader{spec: metricsSpec("alerts")}}, "--metrics-url", server.URL); err == nil {
		t.Fatal("unavailable alerts must fail evaluation")
	}
}

func TestEvaluateOnceRequiresAMetricsURLForMetricsDetectors(t *testing.T) {
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)})
	if err == nil || !strings.Contains(err.Error(), "--metrics-url") || output != "" {
		t.Fatalf("metrics health evaluation without a source must fail: %q %v", output, err)
	}
	if _, err := evaluateOnceWithMetrics(t, []sdk.Detector{metricsDetector("thresholds", errorQuery)}, "--metrics-url", "ftp://nowhere"); err == nil {
		t.Fatal("a metrics URL must be http or https")
	}
}

func TestEvaluateOnceWithoutMetricsDetectorsIgnoresTheMetricsURL(t *testing.T) {
	output, err := evaluateOnceWithMetrics(t, []sdk.Detector{trafficDetector("none", "health")}, "--metrics-url", "http://127.0.0.1:1")
	if err == nil && !strings.Contains(output, "clear") {
		t.Fatalf("unexpected output %q", output)
	}
}
