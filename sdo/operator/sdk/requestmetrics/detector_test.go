package requestmetrics_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
	"sdo.dev/operator/sdk/sdktest"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const (
	pkgRoute = "/api/:version/projects/:id/packages/generic/*"
	webRoute = "/api/:version/projects/:id"
)

func entry() requestmetrics.Entry {
	return requestmetrics.Entry{
		Service: "web", Metric: "http_requests_total", Selector: `{job="web"}`,
		RouteLabel: "endpoint_id", MethodLabel: "method", StatusLabel: "status",
		Recent: "recent", Baseline: "baseline", Presence: "presence",
	}
}

func newDetector() sdk.Detector {
	return requestmetrics.NewDetector(requestmetrics.Config{
		Spec:    sdk.DetectorSpec{ID: "request-metrics"},
		Entries: []requestmetrics.Entry{entry()},
		Operations: []requestmetrics.Operation{
			{ID: "package-download", Service: "web", Route: pkgRoute, Method: "GET"},
			{ID: "project-show", Service: "web", Route: webRoute, Method: "GET"},
		},
		Rule: requestmetrics.DefaultRule(),
	})
}

func series(route, status string, values ...float64) metrics.Series {
	s := metrics.Series{Labels: map[string]string{"endpoint_id": route, "method": "GET", "status": status}}
	for i, v := range values {
		s.Samples = append(s.Samples, metrics.Sample{At: t0.Add(time.Duration(i) * 5 * time.Second), Value: v})
	}
	return s
}

func instant(route, status string, v float64) metrics.Series { return series(route, status, v) }

func snapshot(recent, baseline, presence []metrics.Series) sdktest.Snapshot {
	res := func(id string, series []metrics.Series) metrics.Result {
		return metrics.Result{Query: metrics.Query{ID: id}, ObservedAt: t0.Add(30 * time.Second), Series: series}
	}
	return sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{
		"reqmetrics-web-recent":   res("recent", recent),
		"reqmetrics-web-baseline": res("baseline", baseline),
		"reqmetrics-web-presence": res("presence", presence),
	}}
}

// healthy traffic: 200 requests per window on each route, no errors.
func healthyBaseline() []metrics.Series {
	return []metrics.Series{
		instant(pkgRoute, "200", 400), instant(webRoute, "200", 400), instant(webRoute, "404", 4),
	}
}

func presence() []metrics.Series { return []metrics.Series{series(pkgRoute, "200", 1)} }

func detect(t *testing.T, s sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	got, err := newDetector().Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func steady(v float64, n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// The prototype's package-stuck pattern: the generic-package GET starts
// answering 4xx far above its healthy share (8 of 19 in the window).
func TestPackageStuckPatternRaisesNamingTheOperation(t *testing.T) {
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
		series(webRoute, "200", steady(40, 7)...),
	}
	got := detect(t, snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 {
		t.Fatalf("findings = %+v", got)
	}
	f := got[0]
	if f.RuleID != requestmetrics.RuleErrors || f.Metadata["operation"] != "package-download" || f.Metadata["route"] != pkgRoute {
		t.Fatalf("finding does not name the operation and route: %+v", f)
	}
	if f.Metadata["class"] != requestmetrics.ClassClientJump || f.Metadata["failed"].(float64) != 8 || f.Metadata["window"] != "30s" {
		t.Fatalf("counts or window missing: %+v", f.Metadata)
	}
	if !strings.Contains(f.Summary, "package-download") {
		t.Fatalf("summary %q", f.Summary)
	}
}

func TestServerErrorsRaiseOnTwoConsecutiveSteps(t *testing.T) {
	one := []metrics.Series{series(webRoute, "200", steady(40, 7)...), series(webRoute, "500", 0, 0, 0, 0, 0, 0, 9)}
	if got := detect(t, snapshot(one, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("one step raised: %+v", got)
	}
	two := []metrics.Series{series(webRoute, "200", steady(40, 7)...), series(webRoute, "500", 0, 0, 0, 0, 0, 9, 9)}
	got := detect(t, snapshot(two, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["operation"] != "project-show" || got[0].Metadata["class"] != requestmetrics.ClassServer {
		t.Fatalf("findings = %+v", got)
	}
}

func TestHealthyNoiseDoesNotRaise(t *testing.T) {
	// 1% routine 404s and an occasional single 500: below every floor.
	recent := []metrics.Series{
		series(webRoute, "200", steady(60, 7)...),
		series(webRoute, "404", steady(1, 7)...),
		series(webRoute, "500", 0, 1, 0, 0, 1, 0, 0),
		series(pkgRoute, "200", steady(30, 7)...),
	}
	if got := detect(t, snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("healthy noise raised: %+v", got)
	}
}

func TestRoutine4xxAtTheHealthyShareDoesNotRaise(t *testing.T) {
	baseline := []metrics.Series{instant(webRoute, "200", 300), instant(webRoute, "404", 100)} // 25% healthy 404s
	recent := []metrics.Series{series(webRoute, "200", steady(30, 7)...), series(webRoute, "404", steady(10, 7)...)}
	if got := detect(t, snapshot(recent, baseline, presence())); len(got) != 0 {
		t.Fatalf("routine 404s raised: %+v", got)
	}
}

func TestLowVolumeOperationDoesNotRaise(t *testing.T) {
	// 4 of 4 requests fail: all of them, but under the 5-request minimum.
	recent := []metrics.Series{series(webRoute, "500", steady(4, 7)...)}
	if got := detect(t, snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("low volume raised: %+v", got)
	}
}

func TestFewFailuresInHighVolumeDoNotRaise(t *testing.T) {
	// 6 failures of 600 requests is 1%: enough failures, too small a share.
	recent := []metrics.Series{series(webRoute, "200", steady(594, 7)...), series(webRoute, "500", steady(6, 7)...)}
	if got := detect(t, snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("1%% share raised: %+v", got)
	}
}

func TestOtherOperationsFailuresDoNotNameThisOne(t *testing.T) {
	recent := []metrics.Series{series(pkgRoute, "200", steady(30, 7)...), series(webRoute, "500", steady(20, 7)...)}
	got := detect(t, snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["operation"] != "project-show" {
		t.Fatalf("findings = %+v", got)
	}
}

func TestMetricWithoutSeriesIsNotInstrumentedNotSilence(t *testing.T) {
	got := detect(t, snapshot(nil, nil, nil))
	if len(got) != 2 {
		t.Fatalf("findings = %+v", got)
	}
	for _, f := range got {
		if f.RuleID != requestmetrics.RuleNotInstrumented || !strings.Contains(f.Evidence, "http_requests_total") {
			t.Fatalf("finding %+v", f)
		}
	}
}

func TestMissingRouteLabelIsNotInstrumented(t *testing.T) {
	bare := []metrics.Series{{Labels: map[string]string{"status": "200"}, Samples: []metrics.Sample{{At: t0, Value: 1}}}}
	got := detect(t, snapshot(nil, nil, bare))
	if len(got) != 2 || got[0].RuleID != requestmetrics.RuleNotInstrumented || !strings.Contains(got[0].Evidence, `"endpoint_id"`) {
		t.Fatalf("findings = %+v", got)
	}
}

func TestUnmeasurableOperationIsReportedWithItsReason(t *testing.T) {
	d := requestmetrics.NewDetector(requestmetrics.Config{
		Spec:       sdk.DetectorSpec{ID: "request-metrics"},
		Operations: []requestmetrics.Operation{{ID: "upload", Service: "gateway", Route: "/u", Unmeasurable: "no entries record for service gateway"}},
		Rule:       requestmetrics.DefaultRule(),
	})
	got, err := d.Detect(context.Background(), sdktest.Snapshot{NamespaceName: "app"})
	if err != nil || len(got) != 1 || !strings.Contains(got[0].Evidence, "no entries record") {
		t.Fatalf("findings = %+v, %v", got, err)
	}
}

func TestUnavailableMetricsAreAnErrorNotAClear(t *testing.T) {
	s := snapshot(nil, nil, nil)
	r := s.Metrics["reqmetrics-web-recent"]
	r.Error = "prometheus down"
	s.Metrics["reqmetrics-web-recent"] = r
	if _, err := newDetector().Detect(context.Background(), s); err == nil || !strings.Contains(err.Error(), "prometheus down") {
		t.Fatalf("err = %v", err)
	}
}

func TestSameSnapshotGivesSameFindings(t *testing.T) {
	recent := []metrics.Series{series(webRoute, "200", steady(40, 7)...), series(webRoute, "500", 0, 0, 0, 0, 0, 9, 9)}
	s := snapshot(recent, healthyBaseline(), presence())
	a, b := detect(t, s), detect(t, s)
	if len(a) != 1 || a[0].Evidence != b[0].Evidence || a[0].Fingerprint != b[0].Fingerprint {
		t.Fatalf("replay differs: %+v vs %+v", a, b)
	}
}

func TestDeclaredQueriesAreRangeAndInstant(t *testing.T) {
	d := newDetector().(metrics.Consumer)
	qs := d.MetricQueries()
	if len(qs) != 3 || metrics.ValidateQueries(qs) != nil {
		t.Fatalf("queries = %+v", qs)
	}
	var ranged int
	for _, q := range qs {
		if q.Range > 0 {
			ranged++
		}
	}
	if ranged != 1 {
		t.Fatalf("want one range query, got %d", ranged)
	}
}

func TestInvalidConfigIsReported(t *testing.T) {
	d := requestmetrics.NewDetector(requestmetrics.Config{Spec: sdk.DetectorSpec{ID: "x"}})
	if v, ok := d.(metrics.Validator); !ok || v.Validate() == nil {
		t.Fatal("zero rule must be invalid")
	}
}

const otherRoute = "/api/:version/projects/:id/archive/*"

func undeclaredDetector(max int) sdk.Detector {
	return requestmetrics.NewDetector(requestmetrics.Config{
		Spec:                sdk.DetectorSpec{ID: "request-metrics"},
		Entries:             []requestmetrics.Entry{entry()},
		Operations:          []requestmetrics.Operation{{ID: "project-show", Service: "web", Route: webRoute, Method: "GET"}},
		Rule:                requestmetrics.DefaultRule(),
		MaxUndeclaredRoutes: max,
	})
}

func detectWith(t *testing.T, d sdk.Detector, s sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	got, err := d.Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestUndeclaredRoutePackageStuckPatternRaisesNamingTheRoute(t *testing.T) {
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
		series(webRoute, "200", steady(40, 7)...),
	}
	got := detectWith(t, undeclaredDetector(0), snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 {
		t.Fatalf("findings = %+v", got)
	}
	f := got[0]
	if f.RuleID != requestmetrics.RuleErrors || f.Metadata["route"] != pkgRoute || f.Metadata["undeclared_route"] != true {
		t.Fatalf("finding does not name the undeclared route: %+v", f)
	}
	if !strings.Contains(f.Evidence, "undeclared route") || !strings.Contains(f.Summary, pkgRoute) {
		t.Fatalf("not marked: %q / %q", f.Summary, f.Evidence)
	}
}

func TestHealthyUndeclaredRoutesDoNotRaise(t *testing.T) {
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(30, 7)...),
		series(pkgRoute, "404", steady(1, 7)...),
		series(otherRoute, "200", steady(30, 7)...),
		series(webRoute, "200", steady(40, 7)...),
	}
	if got := detectWith(t, undeclaredDetector(0), snapshot(recent, healthyBaseline(), presence())); len(got) != 0 {
		t.Fatalf("healthy undeclared routes raised: %+v", got)
	}
}

func TestUndeclaredRoutesAreCappedBusiestFirst(t *testing.T) {
	// Both routes fail; with a cap of one only the busier is evaluated, and says so.
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(50, 7)...),
		series(pkgRoute, "500", steady(30, 7)...),
		series(otherRoute, "200", steady(2, 7)...),
		series(otherRoute, "500", steady(20, 7)...),
	}
	s := snapshot(recent, healthyBaseline(), presence())
	got := detectWith(t, undeclaredDetector(1), s)
	if len(got) != 1 || got[0].Metadata["route"] != pkgRoute {
		t.Fatalf("findings = %+v", got)
	}
	if !strings.Contains(got[0].Evidence, "only the 1 busiest of 2") || got[0].Metadata["undeclared_truncated"] == nil {
		t.Fatalf("truncation not stated: %+v", got[0])
	}
	if all := detectWith(t, undeclaredDetector(2), s); len(all) != 2 || all[0].Metadata["undeclared_truncated"] != nil {
		t.Fatalf("cap 2 findings = %+v", all)
	}
}

func TestDeclaredOperationIsNotAlsoEvaluatedAsUndeclared(t *testing.T) {
	recent := []metrics.Series{series(webRoute, "200", steady(40, 7)...), series(webRoute, "500", 0, 0, 0, 0, 0, 9, 9)}
	got := detectWith(t, undeclaredDetector(0), snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["operation"] != "project-show" || got[0].Metadata["undeclared_route"] != nil {
		t.Fatalf("findings = %+v", got)
	}
}
