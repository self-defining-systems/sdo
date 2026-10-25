package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/prometheus"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

type breadthView struct {
	program.ReadView
	calls atomic.Int32
	fail  bool
}

func (v *breadthView) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	v.calls.Add(1)
	if v.fail {
		return "", errors.New("logs unavailable")
	}
	return "broken", nil
}
func (v *breadthView) ProbeTCP(sdk.ObjectRef, int32) (program.ProbeResult, error) {
	v.calls.Add(1)
	return program.ProbeResult{Success: false, Detail: "refused"}, nil
}
func (v *breadthView) ProbeHTTP(sdk.ObjectRef, program.HTTPProbe) (program.ProbeResult, error) {
	v.calls.Add(1)
	return program.ProbeResult{Success: true}, nil
}
func (v *breadthView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	v.calls.Add(1)
	return program.Diff{Changed: true, Text: "source"}, nil
}
func (v *breadthView) DiffAgainstBaseline(sdk.ObjectRef) (program.Diff, error) {
	v.calls.Add(1)
	return program.Diff{Changed: true, Text: "baseline"}, nil
}
func breadthReads(t *testing.T, v program.ReadView) {
	t.Helper()
	ref := sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "web"}
	if r, e := v.Logs(ref, "web", time.Minute); r != "broken" || e != nil {
		t.Errorf("logs=%q %v", r, e)
	}
	if r, e := v.ProbeTCP(ref, 80); r.Success || r.Detail != "refused" || e != nil {
		t.Errorf("tcp=%+v %v", r, e)
	}
	if r, e := v.ProbeHTTP(ref, program.HTTPProbe{Method: "GET", Path: "/", Headers: map[string]string{"X-Test": "1"}}); !r.Success || e != nil {
		t.Errorf("http=%+v %v", r, e)
	}
	if r, e := v.DiffAgainstSource(ref); r.Text != "source" || e != nil {
		t.Errorf("source=%+v %v", r, e)
	}
	if r, e := v.DiffAgainstBaseline(ref); r.Text != "baseline" || e != nil {
		t.Errorf("baseline=%+v %v", r, e)
	}
}
func TestCycleReadDedupeAndSnapshotReplay(t *testing.T) {
	raw := &breadthView{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})}
	cycle := NewCycleReadView(raw)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); breadthReads(t, cycle) }()
	}
	wg.Wait()
	if raw.calls.Load() != 5 {
		t.Fatalf("source calls=%d want 5", raw.calls.Load())
	}
	captured, err := sdktest.CaptureSnapshot(cycle, "app")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	var frozen sdktest.Snapshot
	if err = json.Unmarshal(data, &frozen); err != nil {
		t.Fatal(err)
	}
	replay := &sdktest.ReplayReadView{Snapshot: frozen, Calls: frozen.Readings}
	for range 4 {
		breadthReads(t, replay)
	}
	breadthReads(t, NewCycleReadView(raw))
	if raw.calls.Load() != 10 {
		t.Fatalf("new cycle reused old reads: %d", raw.calls.Load())
	}
}

type ignoringBreadthDetector struct{}

func (ignoringBreadthDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "ignore-error"} }
func (ignoringBreadthDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	_, _ = program.ReadFrom(s).Logs(sdk.ObjectRef{}, "", 0)
	return nil, nil
}
func TestCycleReadFailureNeverClearsEvenWhenIgnored(t *testing.T) {
	raw := &breadthView{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), fail: true}
	cycle := NewCycleReadView(raw)
	for range 2 {
		if _, err := sdk.DetectObserved(context.Background(), ignoringBreadthDetector{}, cycle); err == nil {
			t.Fatal("failed read cleared detector")
		}
	}
	if raw.calls.Load() != 1 {
		t.Fatal("failed read not cached")
	}
	captured, err := sdktest.CaptureSnapshot(cycle, "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.DetectObserved(context.Background(), ignoringBreadthDetector{}, captured); err == nil {
		t.Fatal("replayed failure cleared detector")
	}
}
func TestCycleReadRecordsMetrics(t *testing.T) {
	q := metrics.Query{ID: "up", Expr: "up"}
	frozen := sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{"up": {Query: q, Series: []metrics.Series{{Samples: []metrics.Sample{{Value: 3}}}}}}, Alerts: &metrics.AlertsResult{Error: "offline"}}
	frozen.Traffic = map[string]traffic.Window{"web": {LoadError: "not observable"}}
	v := NewCycleReadView(program.ReadFrom(frozen))
	_ = v.QueryMetrics(q)
	_, _ = v.MetricResult("up")
	_, _ = v.AlertsResult()
	_, _ = v.TrafficWindow("web")
	captured, e := sdktest.CaptureSnapshot(v, "app")
	if e != nil {
		t.Fatal(e)
	}
	data, _ := json.Marshal(captured)
	var replay sdktest.Snapshot
	if e = json.Unmarshal(data, &replay); e != nil {
		t.Fatal(e)
	}
	if r := replay.QueryMetrics(q); len(r.Series) != 1 || r.Series[0].Samples[0].Value != 3 {
		t.Fatalf("metrics lost: %+v", r)
	}
	if r, ok := replay.AlertsResult(); !ok || r.Error != "offline" {
		t.Fatalf("alerts lost: %+v %v", r, ok)
	}
	if r, ok := replay.TrafficWindow("web"); !ok || r.LoadError != "not observable" {
		t.Fatalf("traffic lost: %+v %v", r, ok)
	}
	dir := t.TempDir()
	if e = os.WriteFile(filepath.Join(dir, "snapshot.json"), data, 0600); e != nil {
		t.Fatal(e)
	}
	loaded, e := sdktest.LoadSnapshots(dir)
	if e != nil || len(loaded) != 1 {
		t.Fatalf("strict snapshot loader: %v %v", loaded, e)
	}
	if r := program.ReadFrom(loaded[0]).QueryMetrics(q); r.Error != "" || len(r.Series) != 1 {
		t.Fatalf("loaded metrics=%+v", r)
	}

}

func TestReadCycleOnDemandMetricsWithoutDeclaredDetector(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries["up"] = `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"job":"web"},"value":[1,"1"]}]}}`
	fake.alerts = `{"status":"success","data":{"alerts":[]}}`
	provider, _, err := withMetrics(staticProvider{sdktest.Snapshot{NamespaceName: "app"}}, nil, server.URL, "", false)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		snapshot, e := provider.Snapshot(context.Background())
		if e != nil {
			t.Fatal(e)
		}
		view := program.ReadFrom(snapshot)
		for range 2 {
			r := view.QueryMetrics(metrics.Query{ID: "up", Expr: "up"})
			if r.Error != "" || len(r.Series) != 1 {
				t.Fatalf("query=%+v", r)
			}
			if r, ok := view.AlertsResult(); !ok || r.Error != "" {
				t.Fatalf("alerts=%+v %v", r, ok)
			}
		}
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.requests) != 4 {
		t.Fatalf("requests=%d want 4", len(fake.requests))
	}
}

// Exercise actual controller snapshot capture, rather than only a recorder.
type logHealthDetector struct{}

func (logHealthDetector) Spec() sdk.DetectorSpec {
	s := metricsSpec("log-health")
	s.Batching.Severity = sdk.SeverityWarning
	return s
}
func (logHealthDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	text, err := program.ReadFrom(s).Logs(sdk.ObjectRef{Kind: "Pod", Namespace: s.Namespace(), Name: "web"}, "web", time.Minute)
	if err != nil {
		return nil, err
	}
	if text == "broken" {
		f := stateFinding("log-health")
		return []sdk.Finding{f}, nil
	}
	return nil, nil
}
func TestControllerReadSnapshotCaptureReplaysDetector(t *testing.T) {
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = t.TempDir()
	raw := &breadthView{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})}
	provider := ReadSnapshotProvider{Base: staticProvider{sdktest.Snapshot{NamespaceName: "app"}}, View: func(sdk.DetectionContext) program.ReadView { return raw }}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{logHealthDetector{}}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if err = controller.Step(context.Background(), time.Unix(1, 0), nil); err != nil {
		t.Fatal(err)
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	dir, err := memory.IncidentCorpusDir(config.CorpusSessionRoot, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@detect.json"))
	if err != nil {
		t.Fatal(err)
	}
	var frozen sdktest.Snapshot
	if err = json.Unmarshal(data, &frozen); err != nil {
		t.Fatal(err)
	}
	fs, err := sdk.DetectObserved(context.Background(), logHealthDetector{}, frozen)
	if err != nil || len(fs) != 1 {
		t.Fatalf("captured detector: %v %v", fs, err)
	}
}

func TestRecordingWrapperPreservesDetectorFailureGuard(t *testing.T) {
	raw := &breadthView{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), fail: true}
	recorded := &sdktest.RecordingReadView{ReadView: NewCycleReadView(raw)}
	if _, err := sdk.DetectObserved(context.Background(), ignoringBreadthDetector{}, recorded); err == nil {
		t.Fatal("recording wrapper hid failed detector read")
	}
}

type oversizedLogs struct{ program.ReadView }

func (oversizedLogs) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return strings.Repeat("x", 64*1024+1), nil
}
func TestCycleReadRejectsOversizedLogs(t *testing.T) {
	v := NewCycleReadView(oversizedLogs{program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})})
	if data, err := v.Logs(sdk.ObjectRef{}, "", 0); err == nil || data != "" {
		t.Fatalf("oversized logs returned: %d %v", len(data), err)
	}
}

type blockedProbe struct {
	program.ReadView
	release chan struct{}
}

func (v blockedProbe) ProbeTCP(sdk.ObjectRef, int32) (program.ProbeResult, error) {
	<-v.release
	return program.ProbeResult{Success: true}, nil
}
func TestCycleReadProbeTimeoutIsRecorded(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	v := NewCycleReadView(blockedProbe{program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), release})
	started := time.Now()
	if _, err := v.ProbeTCP(sdk.ObjectRef{}, 80); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe error=%v", err)
	}
	if time.Since(started) > 10*time.Second {
		t.Fatal("probe timeout not enforced")
	}
	captured, err := sdktest.CaptureSnapshot(v, "app")
	if err != nil {
		t.Fatal(err)
	}
	replay := program.ReadFrom(captured)
	if _, err = replay.ProbeTCP(sdk.ObjectRef{}, 80); err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("timeout lost during replay: %v", err)
	}
}

func TestMetricsDeduplicatesEquivalentQueriesAcrossIDs(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.queries["up"] = `{"status":"success","data":{"resultType":"vector","result":[]}}`
	a := metrics.Query{ID: "a", Expr: "up"}
	b := metrics.Query{ID: "b", Expr: "up"}
	snapshot, _ := snapshotWith(t, prometheus.Client{BaseURL: server.URL}, []sdk.Detector{metricsDetector("a", a), metricsDetector("b", b)}, MetricsObserverOptions{})
	view := program.ReadFrom(snapshot)
	if r := view.QueryMetrics(a); r.Error != "" || r.Query != a {
		t.Fatal(r)
	}
	if r := view.QueryMetrics(b); r.Error != "" || r.Query != b {
		t.Fatal(r)
	}
	q := metrics.Query{ID: "diagnose", Expr: "up"}
	if r := view.QueryMetrics(q); r.Error != "" || r.Query != q {
		t.Fatal(r)
	}
	if len(fake.paths()) != 1 {
		t.Fatalf("equivalent query fetched %d times", len(fake.paths()))
	}
}

type ignoringQueryDetector struct{}

func (ignoringQueryDetector) Spec() sdk.DetectorSpec { return metricsSpec("ignored-query") }
func (ignoringQueryDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	_ = program.ReadFrom(s).QueryMetrics(metrics.Query{ID: "up", Expr: "up"})
	return nil, nil
}
func TestEvaluateOnceSharedReadsCannotIgnoreMetricsFailure(t *testing.T) {
	fake, server := newFakePrometheus(t)
	fake.status = 500
	if _, err := evaluateOnceWithMetrics(t, []sdk.Detector{ignoringQueryDetector{}}, "--metrics-url", server.URL); err == nil {
		t.Fatal("evaluate-once cleared an unreadable query")
	}
}
