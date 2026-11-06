package metrics_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
)

func TestQuerySourceIsOmittedFromJSONWhenEmpty(t *testing.T) {
	plain, err := json.Marshal(metrics.Query{ID: "q", Expr: "up"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "source") {
		t.Fatalf("a query without a source must stay byte-stable for old captures: %s", plain)
	}
	named, _ := json.Marshal(metrics.Query{ID: "q", Source: "prom", Expr: "up"})
	var back metrics.Query
	if err := json.Unmarshal(named, &back); err != nil || back.Source != "prom" {
		t.Fatalf("round trip = %+v, %v (%s)", back, err, named)
	}
}

func TestQuerySourceMustBeASourceName(t *testing.T) {
	for _, bad := range []string{"Prom", "1prom", "a b", "p/q", strings.Repeat("a", 32)} {
		if err := (metrics.Query{ID: "q", Source: bad, Expr: "up"}).Validate(); err == nil {
			t.Errorf("source %q accepted", bad)
		}
	}
	for _, good := range []string{"", "default", "prom-2"} {
		if err := (metrics.Query{ID: "q", Source: good, Expr: "up"}).Validate(); err != nil {
			t.Errorf("source %q rejected: %v", good, err)
		}
	}
}

type queryDetector struct {
	id      string
	queries []metrics.Query
}

func (d queryDetector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id} }
func (d queryDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (d queryDetector) MetricQueries() []metrics.Query { return d.queries }

func TestSameQueryIDOnDifferentSourcesConflicts(t *testing.T) {
	a := queryDetector{id: "a", queries: []metrics.Query{{ID: "q", Source: "one", Expr: "up"}}}
	b := queryDetector{id: "b", queries: []metrics.Query{{ID: "q", Source: "two", Expr: "up"}}}
	if _, err := metrics.Queries([]sdk.Detector{a, b}); err == nil {
		t.Fatal("one query id answered by two sources must conflict")
	}
	if got, err := metrics.Queries([]sdk.Detector{a, a}); err != nil || len(got) != 1 {
		t.Fatalf("identical declarations dedupe: %v, %v", got, err)
	}
}
