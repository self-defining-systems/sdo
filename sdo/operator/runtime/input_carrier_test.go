package runtime

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
)

func TestObserverCompositionPreservesInputs(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at, Traffic: map[string]traffic.Window{"health": {LoadError: "traffic sentinel"}}, Metrics: map[string]metrics.Result{"q": {Error: "metric sentinel"}}, Logs: map[string]logs.Result{"q": {Error: "log sentinel"}}}
	var provider SnapshotProvider = core.StaticSnapshotProvider{SnapshotValue: base}
	provider = LogsSnapshotProvider{Base: provider, Observer: &LogsObserver{clock: func() time.Time { return at }}}
	provider = TrafficSnapshotProvider{Base: provider}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := traffic.WindowFrom(view, "health"); !ok || w.LoadError != "traffic sentinel" {
		t.Errorf("traffic input dropped: %#v, %v", w, ok)
	}
	if r, ok := metrics.ResultFrom(view, "q"); !ok || r.Error != "metric sentinel" {
		t.Errorf("metric input dropped: %#v, %v", r, ok)
	}
	if sdk.ObservationTime(view) != at {
		t.Fatal("clock dropped")
	}
}

// Every ordered subset of observers must preserve every input. Compare to the
// same observer alone, including absent inputs and collected error results.
func TestAllObserverOrdersAndSubsets(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := sdktest.Snapshot{NamespaceName: "app", ObservedAtTime: &at}
	compose := []func(SnapshotProvider) SnapshotProvider{
		func(p SnapshotProvider) SnapshotProvider {
			return TrafficSnapshotProvider{Base: p, Observer: &TrafficObserver{workloads: []string{"health"}, windows: map[string]traffic.Window{"health": {LoadError: "traffic error"}}}, Reachability: &ServiceReachabilityObserver{}}
		},
		func(p SnapshotProvider) SnapshotProvider {
			return MetricsSnapshotProvider{Base: p, Observer: &MetricsObserver{queries: []metrics.Query{{ID: "q", Expr: "up"}}, alerts: true, clock: func() time.Time { return at }}}
		},
		func(p SnapshotProvider) SnapshotProvider {
			return &usageSnapshotProvider{Base: p, clock: func() time.Time { return at }}
		},
		func(p SnapshotProvider) SnapshotProvider {
			return LogsSnapshotProvider{Base: p, Observer: &LogsObserver{queries: []logs.Query{{ID: "q"}}, clock: func() time.Time { return at }}}
		},
	}
	read := func(v sdk.DetectionContext) []any {
		s := inputs.New(v)
		w, wp := s.TrafficWindow("health")
		r, rp := s.ServiceReachability()
		if rp {
			if r.ObservedAt.IsZero() {
				t.Fatal("reachability clock missing")
			}
			r.ObservedAt = at
		}
		m, mp := s.MetricResult("q")
		a, ap := s.AlertsResult()
		u, up := s.UsageResult()
		l, lp := s.LogResult("q")
		return []any{s.ObservedAt(), w, wp, r, rp, m, mp, a, ap, u, up, l, lp}
	}
	expected := read(base)
	owned := [][]int{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10}, {11, 12}}
	for i, makeProvider := range compose {
		v, err := makeProvider(core.StaticSnapshotProvider{SnapshotValue: base}).Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		values := read(v)
		for _, j := range owned[i] {
			expected[j] = values[j]
		}
	}
	var visit func([]int, int)
	visit = func(order []int, mask int) {
		var p SnapshotProvider = core.StaticSnapshotProvider{SnapshotValue: base}
		want := read(base)
		for _, i := range order {
			p = compose[i](p)
			for _, j := range owned[i] {
				want[j] = expected[j]
			}
		}
		v, err := p.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := usage.From(v); err == nil {
			t.Fatalf("usage absence or collection error became healthy for %v", order)
		}
		captured, err := sdktest.CaptureSnapshot(v, "app")
		if err != nil {
			t.Fatal(err)
		}
		replayed := &sdktest.ReplayReadView{Snapshot: captured}
		if replayed.ObservedAt() != at {
			t.Fatalf("capture/replay lost clock for %v", order)
		}

		if got := read(v); !reflect.DeepEqual(got, want) {
			t.Fatalf("order %v: got %#v, want %#v", order, got, want)
		}
		// Missing and failed sources must remain detector errors in every order.
		detector := metrics.NewThresholdDetector(sdk.DetectorSpec{ID: "threshold"}, []metrics.ThresholdRule{{Query: metrics.Query{ID: "q", Expr: "up"}, Op: metrics.Greater}})
		_, metricErr := detector.Detect(context.Background(), v)
		reason := "unavailable"
		if mask&2 != 0 {
			reason = "not configured"
		}
		if metricErr == nil || !strings.Contains(metricErr.Error(), reason) {
			t.Fatalf("order %v: expected %s detector error, got %v", order, reason, metricErr)
		}
		// Program wrappers, checked diagnosis and recording use the same full input contract.
		live := LiveReadView{Carrier: inputs.New(v)}
		recorded := &sdktest.RecordingReadView{ReadView: live}
		if got := read(recorded); !reflect.DeepEqual(got, want) {
			t.Fatalf("recording drops inputs for %v", order)
		}
		for i := range compose {
			if mask&(1<<i) == 0 {
				visit(append(append([]int(nil), order...), i), mask|1<<i)
			}
		}
	}
	visit(nil, 0)
}
