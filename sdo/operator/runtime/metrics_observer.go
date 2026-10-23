package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/traffic"
)

// DefaultMetricsQueryTimeout bounds each metric query, and the alerts read,
// of one evaluation cycle. The queries run concurrently, so it also bounds
// the cycle.
const DefaultMetricsQueryTimeout = 5 * time.Second

// MetricsAPI is the controller's view of a metrics source. The transport,
// for example runtime/prometheus, lives behind it.
type MetricsAPI interface {
	// Query evaluates query at time at; a query with a Range is a range query ending at at.
	Query(ctx context.Context, query metrics.Query, at time.Time) ([]metrics.Series, error)
	// Alerts reads every pending and firing alert.
	Alerts(ctx context.Context) ([]metrics.Alert, error)
}

// MetricsObserverOptions tunes a MetricsObserver; zero values take defaults.
type MetricsObserverOptions struct {
	// QueryTimeout bounds each query; it defaults to DefaultMetricsQueryTimeout.
	QueryTimeout time.Duration
	// Clock supplies each cycle's single evaluation timestamp; it defaults to time.Now.
	Clock func() time.Time
}

// MetricsObserver evaluates the queries the detectors declare, and reads
// alerts when a detector consumes them, once per evaluation cycle at one
// timestamp, and freezes the answers into the cycle's snapshot. Detectors
// never do I/O: a cycle whose detectors read no metrics sends no request.
//
// A source that cannot answer is a distinct state. Its results carry an Error,
// which detectors turn into a detector error: unobservable metrics are
// neither healthy nor firing.
type MetricsObserver struct {
	api     MetricsAPI
	queries []metrics.Query
	alerts  bool
	timeout time.Duration
	clock   func() time.Time
}

// NewMetricsObserver returns nil when no detector consumes metrics or alerts.
// A nil api is allowed: every consumed input is then unavailable, so a
// detector that needs metrics fails loudly instead of reporting health. It
// returns an error for an invalid or conflicting declaration, or too many
// queries.
func NewMetricsObserver(api MetricsAPI, detectors []sdk.Detector, options MetricsObserverOptions) (*MetricsObserver, error) {
	for _, detector := range detectors {
		if err := metrics.ValidateDetector(detector); err != nil {
			return nil, fmt.Errorf("detector %q: %w", detector.Spec().ID, err)
		}
	}
	queries, err := metrics.Queries(detectors)
	if err != nil {
		return nil, err
	}
	alerts := metrics.ConsumesAlerts(detectors)
	if len(queries) == 0 && !alerts {
		return nil, nil
	}
	if options.QueryTimeout <= 0 {
		options.QueryTimeout = DefaultMetricsQueryTimeout
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &MetricsObserver{api: api, queries: queries, alerts: alerts, timeout: options.QueryTimeout, clock: options.Clock}, nil
}

// Summary describes the consumed inputs for the controller log.
func (o *MetricsObserver) Summary() map[string]any {
	ids := make([]string, 0, len(o.queries))
	for _, query := range o.queries {
		ids = append(ids, query.ID)
	}
	return map[string]any{"queries": ids, "alerts": o.alerts, "source_configured": o.api != nil, "query_timeout": o.timeout.String()}
}

// freeze starts one cycle at the clock's current time. Nothing is fetched
// until a detector first reads a result; all results then come from one
// concurrent fetch at that timestamp and never change.
func (o *MetricsObserver) freeze(ctx context.Context) *frozenMetrics {
	return &frozenMetrics{observer: o, ctx: ctx, at: o.clock().UTC()}
}

type frozenMetrics struct {
	observer *MetricsObserver
	// ctx is the snapshot's context: the fetch is deferred to the first read
	// but must still be cancelled with the controller.
	ctx context.Context
	at  time.Time

	once    sync.Once
	results map[string]metrics.Result
	alerts  *metrics.AlertsResult
}

func (f *frozenMetrics) load() {
	f.once.Do(func() {
		f.results, f.alerts = f.observer.fetch(f.ctx, f.at)
	})
}

func (o *MetricsObserver) fetch(ctx context.Context, at time.Time) (map[string]metrics.Result, *metrics.AlertsResult) {
	results := make(map[string]metrics.Result, len(o.queries))
	var alerts *metrics.AlertsResult
	var mu sync.Mutex
	var wait sync.WaitGroup
	for _, query := range o.queries {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result := o.fetchQuery(ctx, query, at)
			mu.Lock()
			results[query.ID] = result
			mu.Unlock()
		}()
	}
	if o.alerts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			frozen := o.fetchAlerts(ctx, at)
			mu.Lock()
			alerts = &frozen
			mu.Unlock()
		}()
	}
	wait.Wait()
	return results, alerts
}

// call runs one source request under the per-query timeout. The deadline is
// enforced here as well as through the context, so a source that ignores its
// context still cannot hold the evaluation cycle.
func call[T any](ctx context.Context, timeout time.Duration, request func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := request(ctx)
		done <- outcome{value, err}
	}()
	select {
	case result := <-done:
		return result.value, result.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

var errMetricsNotConfigured = errors.New("metrics source not configured (set --metrics-url)")

func (o *MetricsObserver) fetchQuery(ctx context.Context, query metrics.Query, at time.Time) metrics.Result {
	result := metrics.Result{Query: query, ObservedAt: at}
	if o.api == nil {
		result.Error = errMetricsNotConfigured.Error()
		return result
	}
	series, err := call(ctx, o.timeout, func(ctx context.Context) ([]metrics.Series, error) { return o.api.Query(ctx, query, at) })
	if err != nil {
		result.Error = "metrics source unavailable: " + err.Error()
		return result
	}
	if len(series) > metrics.MaxSeries {
		result.Error = fmt.Sprintf("metrics source unavailable: query returned %d series, more than the limit of %d; narrow the expression", len(series), metrics.MaxSeries)
		return result
	}
	samples := 0
	for _, item := range series {
		samples += len(item.Samples)
	}
	if samples > metrics.MaxSamples {
		result.Error = fmt.Sprintf("metrics source unavailable: query returned %d samples, more than the limit of %d; narrow the expression or range", samples, metrics.MaxSamples)
		return result
	}
	sort.SliceStable(series, func(left int, right int) bool {
		return metrics.LabelString(series[left].Labels) < metrics.LabelString(series[right].Labels)
	})
	result.Series = series
	return result
}

func (o *MetricsObserver) fetchAlerts(ctx context.Context, at time.Time) metrics.AlertsResult {
	result := metrics.AlertsResult{ObservedAt: at}
	if o.api == nil {
		result.Error = errMetricsNotConfigured.Error()
		return result
	}
	alerts, err := call(ctx, o.timeout, o.api.Alerts)
	if err != nil {
		result.Error = "metrics source unavailable: " + err.Error()
		return result
	}
	if len(alerts) > metrics.MaxAlerts {
		result.Error = fmt.Sprintf("metrics source unavailable: %d alerts, more than the limit of %d", len(alerts), metrics.MaxAlerts)
		return result
	}
	sort.SliceStable(alerts, func(left int, right int) bool {
		if alerts[left].Name() != alerts[right].Name() {
			return alerts[left].Name() < alerts[right].Name()
		}
		return metrics.LabelString(alerts[left].Labels) < metrics.LabelString(alerts[right].Labels)
	})
	result.Alerts = alerts
	return result
}

// MetricsSnapshotProvider adds frozen metric results to another provider's
// snapshots.
type MetricsSnapshotProvider struct {
	Base     SnapshotProvider
	Observer *MetricsObserver
}

func (p MetricsSnapshotProvider) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	base, err := p.Base.Snapshot(ctx)
	if err != nil || p.Observer == nil {
		return base, err
	}
	return metricsContext{DetectionContext: base, frozen: p.Observer.freeze(ctx)}, nil
}

// metricsContext wraps a snapshot with the cycle's frozen metrics. It keeps
// the wrapped snapshot's other optional views visible, so wrapping never
// hides what capture and replay read.
type metricsContext struct {
	sdk.DetectionContext
	frozen *frozenMetrics
}

func (c metricsContext) MetricResult(id string) (metrics.Result, bool) {
	c.frozen.load()
	result, ok := c.frozen.results[id]
	return result, ok
}

func (c metricsContext) AlertsResult() (metrics.AlertsResult, bool) {
	c.frozen.load()
	if c.frozen.alerts == nil {
		return metrics.AlertsResult{}, false
	}
	return *c.frozen.alerts, true
}

func (c metricsContext) TrafficWindow(workload string) (traffic.Window, bool) {
	return traffic.WindowFrom(c.DetectionContext, workload)
}

func (c metricsContext) StatefulSets() []appsv1.StatefulSet {
	if view, ok := c.DetectionContext.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
		return view.StatefulSets()
	}
	return nil
}

func (c metricsContext) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	if view, ok := c.DetectionContext.(interface {
		HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	}); ok {
		return view.HorizontalPodAutoscalers()
	}
	return nil
}

func (c metricsContext) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	if view, ok := c.DetectionContext.(interface {
		PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	}); ok {
		return view.PodDisruptionBudgets()
	}
	return nil
}
