package memory

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
)

// RequestMetricsDetectorID names the generic detector that reads the
// application's own request metrics per user operation. Its findings are
// clues in shadow mode (the default) and open incidents only when
// detection.request_metrics_shadow is off.
const RequestMetricsDetectorID = "request-metrics"

// MaxRequestMetricEntries bounds the entry services the detector reads. Each
// costs three metric queries and one controller shares metrics.MaxQueries, so
// Go takes at most three quarters of that budget.
const MaxRequestMetricEntries = metrics.MaxQueries * 3 / 4 / 3

const (
	requestMetricsInterval = 10 * time.Second
	// requestMetricsBaselineWindow and Offset place the healthy share before
	// the rule's whole lookback (5 m), so a fault never enters its own
	// baseline while it is being judged: a baseline that absorbed it would
	// clear the alert with users still failing.
	requestMetricsBaselineWindow = "10m"
	requestMetricsBaselineOffset = "6m"
)

// plannedRequestMetrics derives the detector configuration from the accepted
// application map: one entry per service with an entries record that an
// operation enters through, one operation per map operation. An operation the
// map cannot tell apart is kept with the reason, so it is reported rather
// than skipped. A map without operations plans nothing.
func plannedRequestMetrics(root string) (requestmetrics.Config, bool) {
	parsed, _, ok := acceptedMap(root)
	if !ok || len(parsed.Operations) == 0 {
		return requestmetrics.Config{}, false
	}
	rule := requestmetrics.DefaultRule()
	cfg := requestmetrics.Config{Rule: rule}
	used := map[string]bool{}
	for _, op := range parsed.Operations {
		used[op.Entry.Service] = true
	}
	var services []string
	for s := range used {
		if _, has := parsed.EntryFor(s); has {
			services = append(services, s)
		}
	}
	sort.Strings(services)
	if len(services) > MaxRequestMetricEntries {
		services = services[:MaxRequestMetricEntries]
	}
	kept := map[string]bool{}
	for _, s := range services {
		e, _ := parsed.EntryFor(s)
		kept[s] = true
		cfg.Entries = append(cfg.Entries, requestmetrics.Entry{
			Service: s, Metric: e.Metric, Selector: e.SelectorText(),
			RouteLabel: e.Labels.Route, MethodLabel: e.Labels.Method, StatusLabel: e.Labels.Status,
			Recent:   e.IncreaseByOperation("30s", ""),
			Baseline: e.IncreaseByOperation(requestMetricsBaselineWindow, requestMetricsBaselineOffset),
			Presence: e.SeriesByOperation(),
		})
	}
	for _, op := range parsed.Operations {
		o := requestmetrics.Operation{ID: op.ID, Service: op.Entry.Service, Route: op.RouteValue(), Method: op.Entry.Method}
		e, has := parsed.EntryFor(op.Entry.Service)
		switch {
		case !has:
			o.Unmeasurable = fmt.Sprintf("no entries record for service %s: declare its request metric and route, method and status labels", op.Entry.Service)
		case !kept[op.Entry.Service]:
			o.Unmeasurable = fmt.Sprintf("service %s is beyond the %d entry services the request-metrics detector reads", op.Entry.Service, MaxRequestMetricEntries)
		}
		if has && (parsed.Internal(op) || e.Labels.Method == "") {
			o.Method = ""
		}
		cfg.Operations = append(cfg.Operations, o)
	}
	for i := range cfg.Entries {
		e, _ := parsed.EntryFor(cfg.Entries[i].Service)
		cfg.Entries[i].ServerErrors = e.Errors
	}
	return cfg, true
}

// prepareRequestMetricsDetector installs the detector when the environment can
// answer range queries and the map has operations, and removes Go's package
// otherwise.
func prepareRequestMetricsDetector(root string, enabled bool) error {
	cfg, ok := plannedRequestMetrics(root)
	if !enabled || !ok {
		return removeBootstrapDetector(root, RequestMetricsDetectorID)
	}
	manifest := genericManifest(RequestMetricsDetectorID, requestMetricsInterval.String(), []layout.Watch{{APIVersion: metrics.APIVersion, Kind: "Metrics"}, {APIVersion: logs.Watch.APIVersion, Kind: logs.Watch.Kind}})
	manifest.Batching.Severity = string(sdk.SeverityWarning)
	// The rule carries its own persistence (two consecutive steps), so the
	// detector fires on its first observation and clears after two.
	manifest.Persistence = layout.Persistence{Firing: 1, Clearing: 2}
	return writeGenericDetector(root, manifest, requestMetricsSource(cfg))
}

func requestMetricsSource(cfg requestmetrics.Config) string {
	var entries, operations strings.Builder
	for _, e := range cfg.Entries {
		fmt.Fprintf(&entries, "\t\t{Service: %q, Metric: %q, Selector: %q, RouteLabel: %q, MethodLabel: %q, StatusLabel: %q, ServerErrors: %q,\n\t\t\tRecent: %q, Baseline: %q, Presence: %q},\n",
			e.Service, e.Metric, e.Selector, e.RouteLabel, e.MethodLabel, e.StatusLabel, e.ServerErrors, e.Recent, e.Baseline, e.Presence)
	}
	for _, o := range cfg.Operations {
		fmt.Fprintf(&operations, "\t\t{ID: %q, Service: %q, Route: %q, Method: %q, Unmeasurable: %q},\n", o.ID, o.Service, o.Route, o.Method, o.Unmeasurable)
	}
	return fmt.Sprintf(`package requestmetrics

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
)

// New watches the application's request metrics per user operation: failed
// requests that are numerous, a real share and far above the healthy share,
// on consecutive steps. An operation without the declared series or labels is
// reported as not instrumented.
func New() sdk.Detector {
	return requestmetrics.NewDetector(requestmetrics.Config{
		Spec: sdk.DetectorSpec{
			%s
			Watches: []sdk.WatchKind{metrics.Watch, logs.Watch},
			Interval: %d * time.Second,
		},
		Rule: requestmetrics.DefaultRule(),
		Entries: []requestmetrics.Entry{
%s		},
		Operations: []requestmetrics.Operation{
%s		},
	})
}
`, strings.Replace(genericSpecWithSeverity(RequestMetricsDetectorID, "Warning"), "Firing: 2", "Firing: 1", 1), int(requestMetricsInterval.Seconds()), entries.String(), operations.String())
}
