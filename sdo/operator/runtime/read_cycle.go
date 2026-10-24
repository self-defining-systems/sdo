package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

// CycleReadView shares frozen reads across every detector in one snapshot.
// A new snapshot must get a new view. Recording remains outside the cache so
// offline replay sees every logical read, including repeated cached failures.
type CycleReadView struct{ *sdktest.RecordingReadView }

func NewCycleReadView(view program.ReadView) *CycleReadView {
	cached := &cachedReadView{ReadView: view, entries: map[string]*cachedRead{}}
	return &CycleReadView{&sdktest.RecordingReadView{ReadView: cached}}
}
func (v *CycleReadView) ObservedView() (sdk.DetectionContext, func() error) {
	return program.CheckReads(v)
}

type cachedRead struct {
	once sync.Once
	data []byte
	err  error
}
type cachedReadView struct {
	program.ReadView
	mu      sync.Mutex
	entries map[string]*cachedRead
}

func (v *cachedReadView) read(method string, args any, result any, fetch func() (any, error)) error {
	key, err := json.Marshal([]any{method, args})
	if err != nil {
		return err
	}
	v.mu.Lock()
	entry := v.entries[string(key)]
	if entry == nil {
		entry = &cachedRead{}
		v.entries[string(key)] = entry
	}
	v.mu.Unlock()
	entry.once.Do(func() {
		r, err := fetch()
		entry.err = err
		if err == nil {
			entry.data, entry.err = json.Marshal(r)
		}
	})
	if entry.err != nil {
		return entry.err
	}
	return json.Unmarshal(entry.data, result)
}
func (v *cachedReadView) Logs(ref sdk.ObjectRef, container string, since time.Duration) (string, error) {
	var r string
	err := v.read("Logs", []any{ref, container, since}, &r, func() (any, error) {
		data, err := call(context.Background(), 5*time.Second, func(context.Context) (string, error) { return v.ReadView.Logs(ref, container, since) })
		if err == nil && len(data) > 64*1024 {
			return "", fmt.Errorf("logs exceed 64 KiB cap")
		}
		return data, err
	})
	return r, err
}
func (v *cachedReadView) ProbeTCP(ref sdk.ObjectRef, port int32) (program.ProbeResult, error) {
	var r program.ProbeResult
	err := v.read("ProbeTCP", []any{ref, port}, &r, func() (any, error) {
		return call(context.Background(), 5*time.Second, func(context.Context) (program.ProbeResult, error) { return v.ReadView.ProbeTCP(ref, port) })
	})
	return r, err
}
func (v *cachedReadView) ProbeHTTP(ref sdk.ObjectRef, req program.HTTPProbe) (program.ProbeResult, error) {
	var r program.ProbeResult
	err := v.read("ProbeHTTP", []any{ref, req}, &r, func() (any, error) {
		return call(context.Background(), 5*time.Second, func(context.Context) (program.ProbeResult, error) { return v.ReadView.ProbeHTTP(ref, req) })
	})
	return r, err
}
func (v *cachedReadView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	var r program.Diff
	err := v.read("DiffAgainstSource", []any{ref}, &r, func() (any, error) { return v.ReadView.DiffAgainstSource(ref) })
	return r, err
}
func (v *cachedReadView) DiffAgainstBaseline(ref sdk.ObjectRef) (program.Diff, error) {
	var r program.Diff
	err := v.read("DiffAgainstBaseline", []any{ref}, &r, func() (any, error) { return v.ReadView.DiffAgainstBaseline(ref) })
	return r, err
}

// ReadSnapshotProvider supplies the same live surface to detectors, health
// verification and Diagnose. View is evaluated once for each new snapshot.
type ReadSnapshotProvider struct {
	Base SnapshotProvider
	View func(sdk.DetectionContext) program.ReadView
}

func (p ReadSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	s, err := p.Base.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return NewCycleReadView(p.View(s)), nil
}

// Freeze and copy telemetry at the same boundary as primitives so one reader
// cannot mutate the observations subsequently seen by another detector.
type frozenMetricRead struct {
	Result    metrics.Result
	Available bool
}
type frozenAlertRead struct {
	Result    metrics.AlertsResult
	Available bool
}
type frozenTrafficRead struct {
	Result    traffic.Window
	Available bool
}

func (v *cachedReadView) MetricResult(id string) (metrics.Result, bool) {
	var r frozenMetricRead
	err := v.read("MetricResult", []any{id}, &r, func() (any, error) {
		result, ok := v.ReadView.MetricResult(id)
		return frozenMetricRead{result, ok}, nil
	})
	if err != nil {
		return metrics.Result{Error: err.Error()}, true
	}
	return r.Result, r.Available
}
func (v *cachedReadView) AlertsResult() (metrics.AlertsResult, bool) {
	var r frozenAlertRead
	err := v.read("AlertsResult", []any{}, &r, func() (any, error) { result, ok := v.ReadView.AlertsResult(); return frozenAlertRead{result, ok}, nil })
	if err != nil {
		return metrics.AlertsResult{Error: err.Error()}, true
	}
	return r.Result, r.Available
}
func (v *cachedReadView) TrafficWindow(name string) (traffic.Window, bool) {
	var r frozenTrafficRead
	err := v.read("TrafficWindow", []any{name}, &r, func() (any, error) {
		result, ok := v.ReadView.TrafficWindow(name)
		return frozenTrafficRead{result, ok}, nil
	})
	if err != nil {
		return traffic.Window{LoadError: err.Error()}, true
	}
	return r.Result, r.Available
}
func (v *cachedReadView) QueryMetrics(q metrics.Query) metrics.Result {
	var r metrics.Result
	err := v.read("QueryMetrics", []any{q}, &r, func() (any, error) { return v.ReadView.QueryMetrics(q), nil })
	if err != nil {
		return metrics.Result{Query: q, Error: err.Error()}
	}
	return r
}
