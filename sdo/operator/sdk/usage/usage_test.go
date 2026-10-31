package usage_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/usage"
	"strings"
	"testing"
	"time"
)

func TestHelpers(t *testing.T) {
	if _, ok := usage.RatioToLimit(4, nil); ok {
		t.Fatal("absent limit")
	}
	limit := 8.0
	if r, ok := usage.RatioToLimit(4, &limit); !ok || r != .5 {
		t.Fatal(r, ok)
	}
	start := time.Unix(100, 0)
	points := []usage.Point{{At: start, Value: 2}, {At: start.Add(time.Minute), Value: 4}, {At: start.Add(2 * time.Minute), Value: 6}}
	if d, ok := usage.TimeToThreshold(points, 10); !ok || d != 2*time.Minute {
		t.Fatal(d, ok)
	}
	if _, ok := usage.TimeToThreshold([]usage.Point{{At: start, Value: 1}}, 10); ok {
		t.Fatal("one point predicts")
	}
	samples := []usage.Sample{{Periods: 100, ThrottledPeriods: 10, ThrottlingAvailable: true}, {Periods: 200, ThrottledPeriods: 30, ThrottlingAvailable: true}}
	if r, ok := usage.ThrottledFraction(samples); !ok || r != .2 {
		t.Fatal(r, ok)
	}
}
func TestForecastFlatAndReset(t *testing.T) {
	at := time.Unix(1, 0)
	if _, ok := usage.TimeToThreshold([]usage.Point{{At: at, Value: 1}, {At: at.Add(time.Second), Value: 1}}, 2); ok {
		t.Fatal("flat forecast")
	}
	if _, ok := usage.ThrottledFraction([]usage.Sample{{Periods: 100, ThrottledPeriods: 40}, {Periods: 1, ThrottledPeriods: 0}}); ok {
		t.Fatal("reset is not a delta")
	}
}
func TestEvidenceFinding(t *testing.T) {
	limit := 10.0
	forecast := time.Minute
	f := usage.ThresholdFinding("memory", sdk.ObjectRef{Kind: "Pod", Name: "pod"}, sdk.SeverityWarning, 9, &limit, 2*time.Minute, &forecast)
	for _, part := range []string{"observed=9", "limit=10", "window=2m0s", "forecast=1m0s"} {
		if !strings.Contains(f.Evidence, part) {
			t.Fatal(f.Evidence)
		}
	}
}
func TestSyntheticSource(t *testing.T) {
	r := usage.Result{Containers: []usage.ContainerWindow{{Samples: []usage.Sample{{Periods: 1}}}}}
	snapshot := sdktest.Snapshot{Usage: &r}
	got, err := usage.From(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	got.Containers[0].Samples[0].Periods = 4
	again, _ := usage.From(snapshot)
	if again.Containers[0].Samples[0].Periods != 1 {
		t.Fatal("aliased fixture")
	}
	r.Error = "forbidden"
	if _, err := usage.From(snapshot); err == nil {
		t.Fatal("source failure")
	}
}
