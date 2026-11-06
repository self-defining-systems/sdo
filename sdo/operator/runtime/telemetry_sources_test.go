package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

type sourcesDetector struct {
	queries []metrics.Query
	alerts  bool
}

func (d sourcesDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: "synthetic"} }
func (d sourcesDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (d sourcesDetector) MetricQueries() []metrics.Query { return d.queries }
func (d sourcesDetector) ConsumesAlerts() bool           { return d.alerts }

type failingAlerts struct{ *telemetrytest.Fake }

func (failingAlerts) Alerts(context.Context) ([]metrics.Alert, error) {
	return nil, errors.New("backend down")
}

func seriesFor(job string) telemetry.Result {
	at := time.Unix(1_700_000_000, 0).UTC()
	return telemetry.Result{Series: []metrics.Series{{Labels: map[string]string{"job": job}, Samples: []metrics.Sample{{At: at, Value: 1}}}}}
}

func sourceSet(t *testing.T, bound ...telemetry.Bound) MetricsAPI {
	t.Helper()
	resolver, err := telemetry.NewResolver(bound)
	if err != nil {
		t.Fatal(err)
	}
	return newResolverMetrics(resolver)
}

func metricsBound(id string, isDefault bool, client telemetry.Client, caps ...telemetry.Capability) telemetry.Bound {
	return telemetry.Bound{
		Source: telemetry.Source{ID: id, Kinds: []telemetry.Kind{telemetry.KindMetrics}, Adapter: "fake", Default: isDefault, Capabilities: caps},
		Client: client,
	}
}

func fetchAll(t *testing.T, api MetricsAPI, detector sourcesDetector) map[string]metrics.Result {
	t.Helper()
	observer, err := NewMetricsObserver(api, []sdk.Detector{detector}, MetricsObserverOptions{})
	if err != nil || observer == nil {
		t.Fatalf("observer = %v, %v", observer, err)
	}
	results, _ := observer.fetch(context.Background(), time.Unix(1_700_000_000, 0).UTC())
	return results
}

func TestObserverRoutesEachQueryToItsSourceAndTheDefaultServesTheRest(t *testing.T) {
	prom := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("prom"))
	other := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("other"))
	api := sourceSet(t, metricsBound("prom", true, prom), metricsBound("other", false, other))
	results := fetchAll(t, api, sourcesDetector{queries: []metrics.Query{
		{ID: "legacy", Expr: "up"},
		{ID: "named", Source: "other", Expr: "up"},
	}})
	if got := results["legacy"].Series[0].Labels["job"]; got != "prom" {
		t.Errorf("a query without a source went to %q, want the default", got)
	}
	if got := results["named"].Series[0].Labels["job"]; got != "other" {
		t.Errorf("a named query went to %q", got)
	}
	if results["named"].Query.Source != "other" || results["legacy"].Query.Source != "" {
		t.Errorf("results must carry the query as declared: %+v %+v", results["named"].Query, results["legacy"].Query)
	}
}

func TestObserverDoesNotShareReadsAcrossSources(t *testing.T) {
	a := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("a"))
	b := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("b"))
	api := sourceSet(t, metricsBound("a", false, a), metricsBound("b", false, b))
	fetchAll(t, api, sourcesDetector{queries: []metrics.Query{
		{ID: "one", Source: "a", Expr: "up"}, {ID: "two", Source: "b", Expr: "up"}, {ID: "three", Source: "a", Expr: "up"},
	}})
	if len(a.Calls()) != 1 || len(b.Calls()) != 1 {
		t.Fatalf("identical text on different sources must read each source once; a=%d b=%d", len(a.Calls()), len(b.Calls()))
	}
}

func TestObserverNamesTheSourceWhenItCannotAnswer(t *testing.T) {
	a := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("a"))
	b := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("b"))
	down := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})
	down.Set(telemetrytest.BehaviorServerError)
	api := sourceSet(t, metricsBound("a", false, a), metricsBound("b", false, b), metricsBound("down", false, down))
	results := fetchAll(t, api, sourcesDetector{queries: []metrics.Query{
		{ID: "unbound", Source: "zzz", Expr: "up"},
		{ID: "ambiguous", Expr: "up"},
		{ID: "unavailable", Source: "down", Expr: "up"},
	}})
	for id, want := range map[string]string{
		"unbound":     `telemetry source "zzz" is not bound; bound: a, b, down`,
		"ambiguous":   "names no source and no default is set; bound: a, b, down",
		"unavailable": `telemetry source "down" unavailable:`,
	} {
		if got := results[id].Error; !strings.Contains(got, want) || len(results[id].Series) != 0 {
			t.Errorf("%s: error = %q, want it to contain %q", id, got, want)
		}
	}
}

func TestAlertsMergeEveryAlertCapableSourceAndTagThem(t *testing.T) {
	a := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})
	a.SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "A"}, State: "firing"}})
	b := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})
	b.SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "B"}, State: "firing"}})
	plain := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})
	plain.SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "never"}}})
	api := sourceSet(t,
		metricsBound("a", true, a, telemetry.CapAlerts), metricsBound("b", false, b, telemetry.CapAlerts), metricsBound("plain", false, plain))
	alerts, err := api.Alerts(context.Background())
	if err != nil || len(alerts) != 2 {
		t.Fatalf("alerts = %+v, %v", alerts, err)
	}
	got := map[string]string{}
	for _, alert := range alerts {
		got[alert.Name()] = alert.Source
	}
	if got["A"] != "a" || got["B"] != "b" {
		t.Fatalf("alert sources = %v", got)
	}
}

func TestAlertsFailWhenAnySourceFailsAndNameIt(t *testing.T) {
	good := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})
	bad := failingAlerts{telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})}
	api := sourceSet(t, metricsBound("good", true, good, telemetry.CapAlerts), metricsBound("bad", false, bad, telemetry.CapAlerts))
	observer, err := NewMetricsObserver(api, []sdk.Detector{sourcesDetector{alerts: true}}, MetricsObserverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, alerts := observer.fetch(context.Background(), time.Unix(1_700_000_000, 0).UTC())
	if alerts == nil || !strings.Contains(alerts.Error, `telemetry source "bad" unavailable: backend down`) {
		t.Fatalf("alerts = %+v; a failing source must fail the read, naming it", alerts)
	}
}

func TestAlertsWithoutAnyAlertCapableSourceAreUnavailable(t *testing.T) {
	api := sourceSet(t, metricsBound("a", true, telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{})))
	if _, err := api.Alerts(context.Background()); err == nil {
		t.Fatal("no source serves alerts: want an error, not an empty (healthy) list")
	}
}

func TestFlagBindingServesDetectorsWrittenWithoutASource(t *testing.T) {
	fake := telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("flag"))
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	api, err := newMetricsSource(fakeRegistry(t, fake, &endpoint, &env), "http://prom.test:9090", "")
	if err != nil {
		t.Fatal(err)
	}
	results := fetchAll(t, api, sourcesDetector{queries: []metrics.Query{
		{ID: "legacy", Expr: "up"}, {ID: "explicit", Source: "default", Expr: "up2"}, {ID: "other", Source: "prom", Expr: "up3"},
	}})
	if results["legacy"].Error != "" || results["explicit"].Error != "" {
		t.Fatalf("the synthesized default binding must serve empty and \"default\" sources: %+v", results)
	}
	if !strings.Contains(results["other"].Error, `telemetry source "prom" is not bound; bound: default`) {
		t.Fatalf("other = %q", results["other"].Error)
	}
}
