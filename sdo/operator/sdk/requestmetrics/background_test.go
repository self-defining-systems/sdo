package requestmetrics_test

import (
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/requestmetrics"
)

func TestBackgroundEntryFindingsAreMarked(t *testing.T) {
	e := entry()
	e.Background = true
	d := requestmetrics.NewDetector(requestmetrics.Config{
		Spec:       sdk.DetectorSpec{ID: "request-metrics"},
		Entries:    []requestmetrics.Entry{e},
		Operations: []requestmetrics.Operation{{ID: "project-show", Service: "web", Route: webRoute, Method: "GET"}},
		Rule:       requestmetrics.DefaultRule(),
	})
	recent := []metrics.Series{
		series(pkgRoute, "200", steady(11, 7)...),
		series(pkgRoute, "404", 0, 0, 0, 5, 8, 8, 8),
		series(webRoute, "200", steady(40, 7)...),
	}
	got := detectWith(t, d, snapshot(recent, healthyBaseline(), presence()))
	if len(got) != 1 || got[0].Metadata["background"] != true {
		t.Fatalf("finding not marked background: %+v", got)
	}
}
