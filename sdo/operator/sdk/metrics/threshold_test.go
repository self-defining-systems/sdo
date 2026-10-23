package metrics_test

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/sdktest"
)

var observedAt = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "metrics-thresholds", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches: []sdk.WatchKind{metrics.Watch}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",
	}
}

func instant(value float64, labels map[string]string) metrics.Series {
	return metrics.Series{Labels: labels, Samples: []metrics.Sample{{At: observedAt, Value: value}}}
}

func result(query metrics.Query, series ...metrics.Series) metrics.Result {
	return metrics.Result{Query: query, ObservedAt: observedAt, Series: series}
}

func snapshot(results ...metrics.Result) sdktest.Snapshot {
	byID := map[string]metrics.Result{}
	for _, item := range results {
		byID[item.Query.ID] = item
	}
	return sdktest.Snapshot{NamespaceName: "shop", Metrics: byID}
}

var errorRate = metrics.Query{ID: "error-rate", Expr: `sum by (deployment) (rate(http_errors_total[1m]))`}

func errorRateRule() metrics.ThresholdRule {
	return metrics.ThresholdRule{Query: errorRate, Op: metrics.Greater, Threshold: 0.05}
}

func TestThresholdComparisons(t *testing.T) {
	cases := []struct {
		op        metrics.Comparison
		value     float64
		threshold float64
		fires     bool
	}{
		{metrics.Greater, 0.06, 0.05, true},
		{metrics.Greater, 0.05, 0.05, false},
		{metrics.GreaterOrEqual, 0.05, 0.05, true},
		{metrics.Less, 1, 2, true},
		{metrics.Less, 2, 2, false},
		{metrics.LessOrEqual, 2, 2, true},
		{metrics.Equal, 3, 3, true},
		{metrics.Equal, 3, 4, false},
		{metrics.NotEqual, 3, 4, true},
		{metrics.NotEqual, 3, 3, false},
		// NaN is no observation: no comparison holds, not even !=.
		{metrics.NotEqual, math.NaN(), 3, false},
		{metrics.Greater, math.NaN(), 3, false},
		{metrics.Greater, math.Inf(1), 3, true},
	}
	for _, tc := range cases {
		rule := metrics.ThresholdRule{Query: errorRate, Op: tc.op, Threshold: tc.threshold}
		detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{rule})
		findings, err := detector.Detect(context.Background(), snapshot(result(errorRate, instant(tc.value, map[string]string{"deployment": "web"}))))
		if err != nil {
			t.Fatalf("%v %v %v: %v", tc.value, tc.op, tc.threshold, err)
		}
		if (len(findings) == 1) != tc.fires {
			t.Errorf("%v %s %v: fires=%v, findings %d", tc.value, tc.op, tc.threshold, tc.fires, len(findings))
		}
	}
}

func TestThresholdFindingNamesTheMappedResourceAndCarriesEvidence(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate,
		instant(0.4, map[string]string{"namespace": "shop", "deployment": "checkout", "pod": "checkout-abc", "service": "checkout"}))))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 {
		t.Fatalf("findings: %+v", findings)
	}
	finding := findings[0]
	if finding.DetectorID != "metrics-thresholds" || finding.RuleID != "metric-threshold.error-rate" ||
		finding.Status != sdk.FindingActive || finding.Severity != sdk.SeverityCritical {
		t.Fatalf("finding identity: %+v", finding)
	}
	if want := (sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "shop", Name: "checkout-abc"}); finding.PrimaryResource != want {
		t.Fatalf("primary resource: %+v", finding.PrimaryResource)
	}
	related := map[sdk.ObjectRef]bool{}
	for _, ref := range finding.RelatedResources {
		related[ref] = true
	}
	if !related[sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "checkout"}] ||
		!related[sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "shop", Name: "checkout"}] || len(related) != 2 {
		t.Fatalf("related resources: %+v", finding.RelatedResources)
	}
	for _, want := range []string{"error-rate", errorRate.Expr, "0.4", "> 0.05", `deployment="checkout"`, "2026-10-01T12:00:00Z"} {
		if !strings.Contains(finding.Evidence, want) {
			t.Errorf("evidence lacks %q: %s", want, finding.Evidence)
		}
	}
	if finding.Metadata["query_id"] != "error-rate" || finding.Metadata["value"] != 0.4 || finding.Metadata["threshold"] != 0.05 ||
		finding.Metadata["comparison"] != ">" {
		t.Fatalf("metadata: %+v", finding.Metadata)
	}
	if labels, _ := finding.Metadata["labels"].(map[string]string); labels["deployment"] != "checkout" {
		t.Fatalf("metadata labels: %+v", finding.Metadata["labels"])
	}
	if len(finding.Playbooks) != 1 || finding.Playbooks[0] != "health-objective" {
		t.Fatalf("playbooks: %+v", finding.Playbooks)
	}
	if _, err := json.Marshal(finding.Metadata); err != nil {
		t.Fatalf("metadata must serialize: %v", err)
	}
}

func TestInfiniteValuesStayJSONSerializable(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate, instant(math.Inf(1), map[string]string{"deployment": "web"}))))
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings %+v err %v", findings, err)
	}
	if _, err := json.Marshal(findings[0].Metadata); err != nil {
		t.Fatalf("metadata must serialize: %v", err)
	}
}

func TestLabelMappingDefaultsAndOverrides(t *testing.T) {
	rule := errorRateRule()
	rule.Labels = metrics.LabelMapping{Namespace: "kubernetes_namespace", Deployment: "app", Pod: "kubernetes_pod_name", Service: "destination_service_name"}
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{rule})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate,
		instant(1, map[string]string{"kubernetes_namespace": "shop", "app": "cart", "deployment": "ignored"}))))
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings %+v err %v", findings, err)
	}
	if want := (sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "shop", Name: "cart"}); findings[0].PrimaryResource != want {
		t.Fatalf("primary resource: %+v", findings[0].PrimaryResource)
	}
}

func TestSeriesWithoutResourceLabelsNameTheNamespace(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate, instant(1, nil))))
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings %+v err %v", findings, err)
	}
	if want := (sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: "shop"}); findings[0].PrimaryResource != want {
		t.Fatalf("primary resource: %+v", findings[0].PrimaryResource)
	}
}

func TestSeriesOfOtherNamespacesAreIgnoredAndCounted(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate,
		instant(1, map[string]string{"namespace": "other", "deployment": "web"}),
		instant(1, map[string]string{"namespace": "shop", "deployment": "api"}))))
	if err != nil || len(findings) != 1 {
		t.Fatalf("findings %+v err %v", findings, err)
	}
	if findings[0].PrimaryResource.Name != "api" || findings[0].Metadata["series_in_other_namespaces"] != 1 {
		t.Fatalf("finding: %+v", findings[0])
	}
}

func TestEachSeriesGetsItsOwnFingerprintAndOutputIsSorted(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	series := []metrics.Series{
		instant(1, map[string]string{"pod": "b", "instance": "10.0.0.2"}),
		instant(1, map[string]string{"pod": "b", "instance": "10.0.0.1"}),
		instant(1, map[string]string{"pod": "a", "instance": "10.0.0.3"}),
	}
	first, err := detector.Detect(context.Background(), snapshot(result(errorRate, series...)))
	if err != nil {
		t.Fatal(err)
	}
	reversed := []metrics.Series{series[2], series[1], series[0]}
	second, err := detector.Detect(context.Background(), snapshot(result(errorRate, reversed...)))
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 3 {
		t.Fatalf("findings: %+v", first)
	}
	seen := map[string]bool{}
	for index := range first {
		if first[index].Fingerprint == "" || seen[first[index].Fingerprint] {
			t.Fatalf("fingerprints must be distinct and set: %+v", first)
		}
		seen[first[index].Fingerprint] = true
		if first[index].Fingerprint != second[index].Fingerprint || first[index].Evidence != second[index].Evidence {
			t.Fatalf("output depends on series order at %d:\n%+v\n%+v", index, first[index], second[index])
		}
	}
}

func TestUnavailableSourceIsAnErrorNotClear(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	cases := map[string]sdk.DetectionContext{
		"query failed": snapshot(metrics.Result{Query: errorRate, ObservedAt: observedAt, Error: "prometheus unavailable: connection refused"}),
		"no result":    snapshot(),
		"no source":    plainContext{},
	}
	for name, ctx := range cases {
		findings, err := detector.Detect(context.Background(), ctx)
		if err == nil {
			t.Errorf("%s: unavailable metrics must be a detector error, got findings %+v", name, findings)
			continue
		}
		if len(findings) != 0 || !strings.Contains(err.Error(), "error-rate") {
			t.Errorf("%s: findings %+v err %v", name, findings, err)
		}
	}
	if _, err := detector.Detect(context.Background(), snapshot(metrics.Result{Query: errorRate, ObservedAt: observedAt, Error: "boom"})); !strings.Contains(err.Error(), "boom") {
		t.Fatalf("the source's reason must be preserved: %v", err)
	}
}

type plainContext struct{ sdk.DetectionContext }

func TestEmptyResultIsHealthy(t *testing.T) {
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate)))
	if err != nil || len(findings) != 0 {
		t.Fatalf("an answered query with no series is not a fault: %+v %v", findings, err)
	}
}

func TestRangeResultsReduceBeforeComparison(t *testing.T) {
	rangeQuery := metrics.Query{ID: "latency", Expr: `histogram_quantile(0.99, rate(latency_bucket[1m]))`, Range: 5 * time.Minute, Step: time.Minute}
	series := metrics.Series{Labels: map[string]string{"service": "web"}, Samples: []metrics.Sample{
		{At: observedAt.Add(-2 * time.Minute), Value: 9}, {At: observedAt.Add(-time.Minute), Value: 1}, {At: observedAt, Value: 2},
	}}
	cases := map[metrics.Reduction]bool{metrics.ReduceLast: false, metrics.ReduceMax: true, metrics.ReduceMin: false, metrics.ReduceAvg: true}
	for reduce, fires := range cases {
		rule := metrics.ThresholdRule{Query: rangeQuery, Op: metrics.Greater, Threshold: 3, Reduce: reduce}
		detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{rule})
		findings, err := detector.Detect(context.Background(), snapshot(result(rangeQuery, series)))
		if err != nil {
			t.Fatal(err)
		}
		if (len(findings) == 1) != fires {
			t.Errorf("reduce %q: fires=%v findings %+v", reduce, fires, findings)
		}
	}
}

func TestSeverityDefaultsToCriticalAndIsConfigurable(t *testing.T) {
	rule := errorRateRule()
	rule.Severity = sdk.SeverityWarning
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{rule})
	findings, err := detector.Detect(context.Background(), snapshot(result(errorRate, instant(1, nil))))
	if err != nil || len(findings) != 1 || findings[0].Severity != sdk.SeverityWarning {
		t.Fatalf("findings %+v err %v", findings, err)
	}
}

func TestDetectorDeclaresItsQueries(t *testing.T) {
	other := metrics.Query{ID: "queue-depth", Expr: `queue_depth`}
	detector := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{
		errorRateRule(),
		{RuleID: "error-rate-critical", Query: errorRate, Op: metrics.Greater, Threshold: 0.5},
		{Query: other, Op: metrics.Greater, Threshold: 10},
	})
	consumer, ok := detector.(metrics.Consumer)
	if !ok {
		t.Fatalf("detector must declare its queries: %#v", detector)
	}
	queries := consumer.MetricQueries()
	if len(queries) != 2 || queries[0].ID != "error-rate" || queries[1].ID != "queue-depth" {
		t.Fatalf("queries must be distinct and sorted: %+v", queries)
	}
	if err := metrics.ValidateDetector(detector); err != nil {
		t.Fatal(err)
	}
	if detector.Spec().ID != "metrics-thresholds" {
		t.Fatalf("spec: %+v", detector.Spec())
	}
}

func TestInvalidRulesFailClosed(t *testing.T) {
	cases := map[string][]metrics.ThresholdRule{
		"no rules":            nil,
		"empty query id":      {{Query: metrics.Query{Expr: "up"}, Op: metrics.Greater}},
		"empty expression":    {{Query: metrics.Query{ID: "a"}, Op: metrics.Greater}},
		"unknown comparison":  {{Query: errorRate, Op: "~"}},
		"unknown reduction":   {{Query: errorRate, Op: metrics.Greater, Reduce: "median"}},
		"duplicate rule id":   {errorRateRule(), errorRateRule()},
		"conflicting queries": {errorRateRule(), {RuleID: "x", Query: metrics.Query{ID: "error-rate", Expr: "other"}, Op: metrics.Greater}},
		"nan threshold":       {{Query: errorRate, Op: metrics.Greater, Threshold: math.NaN()}},
		"bad severity":        {{Query: errorRate, Op: metrics.Greater, Severity: "meh"}},
	}
	for name, rules := range cases {
		detector := metrics.NewThresholdDetector(spec(), rules)
		if err := metrics.ValidateDetector(detector); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
		if findings, err := detector.Detect(context.Background(), snapshot(result(errorRate, instant(1, nil)))); err == nil {
			t.Errorf("%s: a misconfigured detector must error, not report %+v", name, findings)
		}
	}
}

func TestValidateQueries(t *testing.T) {
	good := metrics.Query{ID: "a", Expr: "up"}
	tooMany := make([]metrics.Query, metrics.MaxQueries+1)
	for index := range tooMany {
		tooMany[index] = metrics.Query{ID: string(rune('a'+index%26)) + string(rune('a'+index/26)), Expr: "up"}
	}
	cases := map[string][]metrics.Query{
		"empty id":         {{Expr: "up"}},
		"blank expression": {{ID: "a", Expr: "  "}},
		"duplicate":        {good, good},
		"too many":         tooMany,
		"step no range":    {{ID: "a", Expr: "up", Step: time.Second}},
		"huge range":       {{ID: "a", Expr: "up", Range: 48 * time.Hour}},
		"too many points":  {{ID: "a", Expr: "up", Range: time.Hour, Step: time.Second}},
		"negative range":   {{ID: "a", Expr: "up", Range: -time.Second}},
	}
	for name, queries := range cases {
		if err := metrics.ValidateQueries(queries); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	if err := metrics.ValidateQueries([]metrics.Query{good, {ID: "b", Expr: "up", Range: time.Hour}}); err != nil {
		t.Fatalf("valid queries: %v", err)
	}
	if got := (metrics.Query{ID: "b", Expr: "up", Range: time.Hour}).EffectiveStep(); got != time.Minute {
		t.Fatalf("derived step: %s", got)
	}
}

func TestQueriesMergeDetectorsAndRejectConflicts(t *testing.T) {
	a := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{errorRateRule()})
	b := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{{Query: errorRate, Op: metrics.Greater, Threshold: 1}})
	queries, err := metrics.Queries([]sdk.Detector{a, b})
	if err != nil || len(queries) != 1 {
		t.Fatalf("shared definitions merge: %+v %v", queries, err)
	}
	c := metrics.NewThresholdDetector(spec(), []metrics.ThresholdRule{{Query: metrics.Query{ID: "error-rate", Expr: "different"}, Op: metrics.Greater}})
	if _, err := metrics.Queries([]sdk.Detector{a, c}); err == nil {
		t.Fatal("redefining a query ID must conflict")
	}
}

func TestResultSerializesNonFiniteSamples(t *testing.T) {
	original := metrics.Result{Query: errorRate, ObservedAt: observedAt, Series: []metrics.Series{
		{Labels: map[string]string{"a": "b"}, Samples: []metrics.Sample{{At: observedAt, Value: math.Inf(-1)}, {At: observedAt, Value: 1.5}, {At: observedAt, Value: math.NaN()}}},
	}}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded metrics.Result
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	samples := decoded.Series[0].Samples
	if !math.IsInf(samples[0].Value, -1) || samples[1].Value != 1.5 || !math.IsNaN(samples[2].Value) {
		t.Fatalf("round trip: %s -> %+v", data, samples)
	}
}
