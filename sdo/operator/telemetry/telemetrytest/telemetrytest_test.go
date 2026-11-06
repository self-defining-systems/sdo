package telemetrytest_test

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

var at = time.Unix(1_700_000_000, 0).UTC()

func metricsWant() telemetry.Result {
	return telemetry.Result{Series: []metrics.Series{{
		Labels:  map[string]string{"job": "web"},
		Samples: []metrics.Sample{{At: at, Value: 1}},
	}}}
}

func logsWant() telemetry.Result {
	return telemetry.Result{Lines: []telemetry.Line{{At: at, Text: "hello", Labels: map[string]string{"app": "web"}}}}
}

func harness(kind telemetry.Kind, want telemetry.Result, build func(*testing.T, telemetrytest.Behavior) telemetry.Client) telemetrytest.Harness {
	return telemetrytest.Harness{
		Kind:      kind,
		Query:     telemetry.Query{Kind: kind, Text: "q"},
		Want:      want,
		New:       build,
		HangGrace: 300 * time.Millisecond,
	}
}

func fakeFactory(kind telemetry.Kind, want telemetry.Result) func(*testing.T, telemetrytest.Behavior) telemetry.Client {
	return func(_ *testing.T, b telemetrytest.Behavior) telemetry.Client {
		f := telemetrytest.NewFake(kind, want)
		f.Set(b)
		return f
	}
}

func TestFakePassesConformance(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
		telemetrytest.TestClient(t, harness(telemetry.KindMetrics, metricsWant(), fakeFactory(telemetry.KindMetrics, metricsWant())), "fake")
	})
	t.Run("logs", func(t *testing.T) {
		telemetrytest.TestClient(t, harness(telemetry.KindLogs, logsWant(), fakeFactory(telemetry.KindLogs, logsWant())), "fake")
	})
}

// broken wraps a conforming fake and applies one deliberate defect.
type broken struct {
	inner telemetry.Client
	b     telemetrytest.Behavior
	fault func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error)
}

func (c broken) Query(ctx context.Context, q telemetry.Query, a time.Time) (telemetry.Result, error) {
	res, err := c.inner.Query(ctx, q, a)
	return c.fault(c.b, res, err)
}

func TestSuiteCatchesBrokenClients(t *testing.T) {
	want := metricsWant()
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	nan := telemetry.Result{Series: []metrics.Series{{Samples: []metrics.Sample{{At: at, Value: math.NaN()}}}}}
	faults := map[string]struct {
		fault func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error)
		fails string
	}{
		"swallows errors into an empty result": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				if err != nil && b != telemetrytest.BehaviorHang {
					return telemetry.Result{}, nil
				}
				return res, err
			}, "server-error-is-error"},
		"treats no data as an error": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				if b == telemetrytest.BehaviorEmpty {
					return res, context.DeadlineExceeded
				}
				return res, err
			}, "no-data-is-empty-not-error"},
		"ignores its context": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				if b == telemetrytest.BehaviorHang || err == context.Canceled {
					<-stuck
					return telemetry.Result{}, nil
				}
				return res, err
			}, "honors-context-deadline"},
		"leaks non-finite values": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				if b == telemetrytest.BehaviorNonFinite {
					return nan, nil
				}
				return res, err
			}, "non-finite"},
		"truncates silently": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				res.Truncated = false
				return res, err
			}, "bounds"},
		"returns the wrong data": {
			func(b telemetrytest.Behavior, res telemetry.Result, err error) (telemetry.Result, error) {
				if b == telemetrytest.BehaviorData {
					res.Series = nil
				}
				return res, err
			}, "data"},
	}
	for name, f := range faults {
		t.Run(name, func(t *testing.T) {
			h := harness(telemetry.KindMetrics, want, func(t *testing.T, b telemetrytest.Behavior) telemetry.Client {
				return broken{inner: fakeFactory(telemetry.KindMetrics, want)(t, b), b: b, fault: f.fault}
			})
			got := telemetrytest.Violations(t, h)
			if _, ok := got[f.fails]; !ok {
				t.Fatalf("check %q did not fail; failures: %v", f.fails, got)
			}
		})
	}
}

func TestCheckNamesAreStable(t *testing.T) {
	want := []string{"data", "no-data-is-empty-not-error", "server-error-is-error", "malformed-is-error", "bounds", "non-finite", "honors-context-deadline", "honors-cancelled-context", "concurrent"}
	if got := telemetrytest.CheckNames(); !slices.Equal(got, want) {
		t.Fatalf("checks = %v", got)
	}
}

func TestFakeRecordsCallsAndAlerts(t *testing.T) {
	f := telemetrytest.NewFake(telemetry.KindMetrics, metricsWant())
	f.SetAlerts([]metrics.Alert{{State: "firing"}})
	if _, err := f.Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"}, at); err != nil {
		t.Fatal(err)
	}
	if calls := f.Calls(); len(calls) != 1 || calls[0].Text != "up" {
		t.Fatalf("calls = %+v", calls)
	}
	var _ telemetry.AlertReader = f
	if a, err := f.Alerts(context.Background()); err != nil || len(a) != 1 {
		t.Fatalf("alerts = %+v err = %v", a, err)
	}
}
