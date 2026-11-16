package requestmetrics_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

// A sum by (route, method, status) leaves the route label off a series whose
// source carries none (a health check, a metrics scrape). Such a series may
// sort first; the metric is instrumented when any series carries the labels.
func TestUnlabelledFirstSeriesDoesNotHideAnInstrumentedMetric(t *testing.T) {
	presence := []metrics.Series{
		{Labels: map[string]string{"method": "GET", "status": "200"}, Samples: []metrics.Sample{{At: t0, Value: 1}}},
		series(pkgRoute, "200", 1),
	}
	for _, f := range detect(t, snapshot(nil, nil, presence)) {
		if f.RuleID == requestmetrics.RuleNotInstrumented {
			t.Fatalf("instrumented metric reported as not instrumented: %+v", f)
		}
	}
}

// An alert that has opened an incident leaves the alert set only through
// confirmed recovery. A reclassification keeps the operation's error
// fingerprint, so an open incident keeps holding on it; the rule stays a clue,
// so a fresh install still opens nothing.
func TestJumpWithAThinBaselineHoldsUntilHealthyRequestsReturn(t *testing.T) {
	baseline := []metrics.Series{instant(pkgRoute, "200", 10), instant(webRoute, "200", 400)}
	held := func(fail, ok []float64) []sdk.Finding {
		recent := []metrics.Series{series(pkgRoute, "200", ok...), series(pkgRoute, "404", fail...), series(webRoute, "200", steady(40, len(ok))...)}
		return detect(t, snapshot(recent, baseline, presence()))
	}
	for name, got := range map[string][]sdk.Finding{
		"failing":            held([]float64{0, 0, 5, 8, 8, 8}, steady(11, 6)),
		"quiet after":        held([]float64{0, 0, 5, 8, 8, 0, 0}, []float64{11, 11, 11, 11, 11, 1, 1}),
		"quiet and no 404 s": held([]float64{0, 0, 5, 8, 8, 0, 0}, []float64{11, 11, 11, 11, 11, 2, 0}),
	} {
		if len(got) != 1 || got[0].RuleID != requestmetrics.RuleInsufficientBaseline || got[0].Fingerprint != "requestmetrics/errors/package-download" {
			t.Fatalf("%s: findings = %+v", name, got)
		}
	}
	if got := held([]float64{0, 0, 5, 8, 8, 0, 0}, []float64{11, 11, 11, 11, 11, 11, 11}); len(got) != 0 {
		t.Fatalf("healthy requests returned but the finding stays: %+v", got)
	}
}

func TestOperationThatStopsBeingInstrumentedKeepsItsErrorFingerprint(t *testing.T) {
	for _, f := range detect(t, snapshot(nil, nil, nil)) {
		if f.RuleID != requestmetrics.RuleNotInstrumented || f.Fingerprint != "requestmetrics/errors/"+f.Metadata["operation"].(string) {
			t.Fatalf("finding %+v", f)
		}
	}
}

const pkgErrorsKey = "request-metrics\x00requestmetrics/errors/package-download"

// pollutedBaseline is a baseline that has absorbed the ongoing fault: the
// route's healthy client share reads 60%.
func pollutedBaseline() []metrics.Series {
	return []metrics.Series{instant(pkgRoute, "200", 40), instant(pkgRoute, "404", 60), instant(webRoute, "200", 400)}
}

func failingFor(steps int) []metrics.Series {
	return []metrics.Series{
		series(pkgRoute, "200", steady(11, steps)...),
		series(pkgRoute, "404", steady(8, steps)...),
		series(webRoute, "200", steady(40, steps)...),
	}
}

// A persistent fault, long after its failures entered the baseline, stays
// raised while the incident it opened is open, and leaves only on healthy
// requests. Without the held key the polluted baseline hides it.
func TestPersistentFaultStaysRaisedWhileItsIncidentIsOpen(t *testing.T) {
	long := failingFor(60) // a 5 s step: five minutes of lookback, 15 minutes into the fault
	free := snapshot(long, pollutedBaseline(), presence())
	if got := detect(t, free); len(got) != 0 {
		t.Fatalf("an unheld finding is judged against the polluted baseline: %+v", got)
	}
	held := snapshot(long, pollutedBaseline(), presence())
	held.HeldKeys = []string{pkgErrorsKey}
	got := detect(t, held)
	if len(got) != 1 || got[0].RuleID != requestmetrics.RuleErrors || got[0].Fingerprint != "requestmetrics/errors/package-download" {
		t.Fatalf("held fault dropped: %+v", got)
	}
	// Quiet traffic does not confirm recovery.
	quiet := []metrics.Series{series(pkgRoute, "200", steady(1, 60)...), series(webRoute, "200", steady(40, 60)...)}
	held = snapshot(quiet, pollutedBaseline(), presence())
	held.HeldKeys = []string{pkgErrorsKey}
	if got := detect(t, held); len(got) != 1 {
		t.Fatalf("no traffic confirmed recovery: %+v", got)
	}
	// Enough healthy requests do.
	healthy := []metrics.Series{series(pkgRoute, "200", steady(11, 60)...), series(webRoute, "200", steady(40, 60)...)}
	held = snapshot(healthy, pollutedBaseline(), presence())
	held.HeldKeys = []string{pkgErrorsKey}
	if got := detect(t, held); len(got) != 0 {
		t.Fatalf("healthy requests did not clear it: %+v", got)
	}
}

// The held keys are a recorded input: a snapshot written and read back gives
// the same verdicts.
func TestHeldKeysReplayWithTheSnapshot(t *testing.T) {
	held := snapshot(failingFor(60), pollutedBaseline(), presence())
	held.HeldKeys = []string{pkgErrorsKey}
	raw, err := json.Marshal(held)
	if err != nil {
		t.Fatal(err)
	}
	var replayed sdktest.Snapshot
	if err := json.Unmarshal(raw, &replayed); err != nil {
		t.Fatal(err)
	}
	want, got := detect(t, held), detect(t, replayed)
	if len(want) != 1 || !reflect.DeepEqual(want, got) {
		t.Fatalf("replay differs:\nrecorded %+v\nreplayed %+v", want, got)
	}
	captured, err := sdktest.CaptureSnapshot(held, "app")
	if err != nil || !reflect.DeepEqual(captured.HeldKeys, held.HeldKeys) {
		t.Fatalf("capture lost the held keys: %v %v", captured.HeldKeys, err)
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

// Routes that carry failures are evaluated before routes that are merely busy: a failing route must not drop out
// of the cap, and so clear without recovery evidence, because healthy routes got busier.
func TestFailingUndeclaredRouteIsNotCappedOutByBusyHealthyRoutes(t *testing.T) {
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "500", 0, 0, 0, 5, 8, 8, 8),
	}
	for i := 0; i < requestmetrics.DefaultMaxUndeclaredRoutes+5; i++ {
		recent = append(recent, series(fmt.Sprintf("GET /busy/%d", i), "200", steady(500, 7)...))
	}
	got := detectWith(t, undeclaredDetector(0), snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["route"] != pkgRoute {
		t.Fatalf("the failing route was capped out: %+v", got)
	}
}

// A fresh install has little healthy history: the 4xx rise is not called a
// jump, the finding says the baseline is insufficient and is not an error
// finding. A server-error rise still raises.
func TestClientJumpNeedsEnoughHealthyBaseline(t *testing.T) {
	baseline := []metrics.Series{instant(pkgRoute, "200", 10), instant(webRoute, "200", 400)}
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
		series(webRoute, "200", steady(40, 7)...),
	}
	got := detect(t, snapshot(recent, baseline, presence()))
	if len(got) != 1 || got[0].RuleID != requestmetrics.RuleInsufficientBaseline || !strings.Contains(got[0].Summary, "insufficient baseline") {
		t.Fatalf("findings = %+v", got)
	}
	server := []metrics.Series{series(pkgRoute, "200", steady(11, 7)...), series(pkgRoute, "500", 0, 0, 0, 5, 8, 8, 8), series(webRoute, "200", steady(40, 7)...)}
	got = detect(t, snapshot(server, baseline, presence()))
	if len(got) != 1 || got[0].RuleID != requestmetrics.RuleErrors {
		t.Fatalf("server errors with a thin baseline = %+v", got)
	}
}

// A low-rate route with a thin, failure-free baseline that then fails for most
// of its requests is called an error, not left a clue that opens no incident.
// The same rise at a smaller share stays the insufficient-baseline clue.
func TestClientJumpOnThinBaselineIsCalledWhenMostRequestsFail(t *testing.T) {
	baseline := []metrics.Series{instant(pkgRoute, "200", 10), instant(webRoute, "200", 400)}
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(5, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 8, 12, 12, 12),
		series(webRoute, "200", steady(40, 7)...),
	}
	got := detect(t, snapshot(recent, baseline, presence()))
	if len(got) != 1 || got[0].RuleID != requestmetrics.RuleErrors {
		t.Fatalf("a mostly failing route on a thin baseline = %+v", got)
	}
	flaky := []metrics.Series{instant(pkgRoute, "200", 6), instant(pkgRoute, "404", 4), instant(webRoute, "200", 400)}
	got = detect(t, snapshot(recent, flaky, presence()))
	if len(got) != 1 || got[0].RuleID != requestmetrics.RuleInsufficientBaseline {
		t.Fatalf("a route already failing in its thin baseline = %+v", got)
	}
}

func TestDeclaredOperationIsNotAlsoEvaluatedAsUndeclared(t *testing.T) {
	recent := []metrics.Series{series(webRoute, "200", steady(40, 7)...), series(webRoute, "500", 0, 0, 0, 0, 0, 9, 9)}
	got := detectWith(t, undeclaredDetector(0), snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["operation"] != "project-show" || got[0].Metadata["undeclared_route"] != nil {
		t.Fatalf("findings = %+v", got)
	}
}

func webPod(started time.Time, ready bool) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", Labels: map[string]string{"app": "web"}, CreationTimestamp: metav1.NewTime(started)},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", StartupProbe: &corev1.Probe{FailureThreshold: 30, PeriodSeconds: 10}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "web", Ready: ready, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}}}}},
	}
}

func restartingSnapshot(started time.Time) sdktest.Snapshot {
	s := snapshot(nil, nil, nil)
	at := t0.Add(2 * time.Minute)
	s.ObservedAtTime = &at
	s.ServiceList = []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "web"}}}}
	s.PodList = []corev1.Pod{webPod(started, false)}
	return s
}

// A metric with no series while the target pod is inside its declared startup
// allowance is a restart, not missing instrumentation.
func TestNoSeriesWhileTargetPodIsStartingIsNotReported(t *testing.T) {
	if got := detect(t, restartingSnapshot(t0.Add(30*time.Second))); len(got) != 0 {
		t.Fatalf("findings = %+v", got)
	}
}

// A target missing beyond its allowance still counts.
func TestNoSeriesBeyondTheStartupAllowanceIsReported(t *testing.T) {
	if got := detect(t, restartingSnapshot(t0.Add(-10*time.Minute))); len(got) != 2 {
		t.Fatalf("findings = %+v", got)
	}
}
