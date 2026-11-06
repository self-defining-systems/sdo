package telemetry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

func bound(id string, def bool, caps []telemetry.Capability, kinds ...telemetry.Kind) telemetry.Bound {
	return telemetry.Bound{
		Source: telemetry.Source{ID: id, Kinds: kinds, Adapter: "fake", Default: def, Capabilities: caps},
		Client: telemetrytest.NewFake(kinds[0], telemetry.Result{}),
	}
}

func TestResolveDefaultSourceRule(t *testing.T) {
	m, l := telemetry.KindMetrics, telemetry.KindLogs
	cases := []struct {
		name    string
		sources []telemetry.Bound
		id      string
		kind    telemetry.Kind
		want    string
		err     error
	}{
		{"named", []telemetry.Bound{bound("prom", true, nil, m), bound("alt", false, nil, m)}, "alt", m, "alt", nil},
		{"empty uses default", []telemetry.Bound{bound("prom", true, nil, m), bound("alt", false, nil, m)}, "", m, "prom", nil},
		{"empty uses sole source without default", []telemetry.Bound{bound("prom", false, nil, m), bound("loki", false, nil, l)}, "", l, "loki", nil},
		{"multi-kind source", []telemetry.Bound{bound("all", true, nil, m, l)}, "", l, "all", nil},
		{"default is per kind", []telemetry.Bound{bound("prom", true, nil, m), bound("loki", true, nil, l)}, "", l, "loki", nil},
		{"unbound name", []telemetry.Bound{bound("prom", true, nil, m)}, "nope", m, "", telemetry.ErrUnbound},
		{"no source serves kind", []telemetry.Bound{bound("prom", true, nil, m)}, "", l, "", telemetry.ErrUnbound},
		{"nothing bound", nil, "", m, "", telemetry.ErrUnbound},
		{"ambiguous without default", []telemetry.Bound{bound("a", false, nil, m), bound("b", false, nil, m)}, "", m, "", telemetry.ErrAmbiguous},
		{"wrong kind for named", []telemetry.Bound{bound("prom", true, nil, m)}, "prom", l, "", telemetry.ErrKind},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := telemetry.NewResolver(c.sources)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.Resolve(c.id, c.kind)
			if !errors.Is(err, c.err) {
				t.Fatalf("err = %v, want %v", err, c.err)
			}
			if got.ID != c.want {
				t.Fatalf("source = %q, want %q", got.ID, c.want)
			}
			var re *telemetry.ResolveError
			if c.err != nil && (!errors.As(err, &re) || !strings.Contains(err.Error(), "bound:")) {
				t.Fatalf("error %q must be a *ResolveError naming what is bound", err)
			}
		})
	}
}

func TestNewResolverRejectsInconsistentSets(t *testing.T) {
	m := telemetry.KindMetrics
	for name, set := range map[string][]telemetry.Bound{
		"duplicate id": {bound("a", false, nil, m), bound("a", false, nil, m)},
		"two defaults": {bound("a", true, nil, m), bound("b", true, nil, m)},
		"bad id":       {bound("Bad_ID", false, nil, m)},
		"unknown kind": {bound("a", false, nil, telemetry.Kind("events"))},
		"no kinds":     {{Source: telemetry.Source{ID: "a"}}},
	} {
		if _, err := telemetry.NewResolver(set); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestResolverQuery(t *testing.T) {
	want := telemetry.Result{Series: []metrics.Series{{Labels: map[string]string{"a": "b"}, Samples: []metrics.Sample{{At: time.Unix(1, 0), Value: 2}}}}}
	fake := telemetrytest.NewFake(telemetry.KindMetrics, want)
	src := telemetry.Source{ID: "prom", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Default: true}
	r, err := telemetry.NewResolver([]telemetry.Bound{{Source: src, Client: fake}})
	if err != nil {
		t.Fatal(err)
	}
	q := telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"}
	res, err := r.Query(context.Background(), q, time.Unix(10, 0))
	if err != nil || len(res.Series) != 1 {
		t.Fatalf("res = %+v err = %v", res, err)
	}
	if got := fake.Calls(); len(got) != 1 || got[0].SourceID != "prom" {
		t.Fatalf("client must see the resolved source id, got %+v", got)
	}

	t.Run("range needs the capability", func(t *testing.T) {
		q := q
		q.Range = time.Minute
		if _, err := r.Query(context.Background(), q, time.Unix(10, 0)); err == nil || !strings.Contains(err.Error(), "range capability") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("client error is unavailable not empty", func(t *testing.T) {
		fake.Set(telemetrytest.BehaviorServerError)
		defer fake.Set(telemetrytest.BehaviorData)
		_, err := r.Query(context.Background(), q, time.Unix(10, 0))
		if err == nil || !strings.Contains(err.Error(), `telemetry source "prom" unavailable`) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("invalid query is rejected before resolution", func(t *testing.T) {
		if _, err := r.Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics}, time.Unix(10, 0)); err == nil {
			t.Fatal("want an error for empty text")
		}
	})
	t.Run("contract-violating result is an error", func(t *testing.T) {
		bad := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{Lines: []telemetry.Line{{Text: "x"}}})
		r, err := telemetry.NewResolver([]telemetry.Bound{{Source: src, Client: bad}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Query(context.Background(), q, time.Unix(10, 0)); err == nil {
			t.Fatal("a metrics query answered with log lines must fail")
		}
	})
}

func TestQueryValidate(t *testing.T) {
	ok := telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*telemetry.Query){
		"unknown kind":       func(q *telemetry.Query) { q.Kind = "x" },
		"blank text":         func(q *telemetry.Query) { q.Text = "  " },
		"huge text":          func(q *telemetry.Query) { q.Text = strings.Repeat("a", telemetry.MaxTextBytes+1) },
		"negative range":     func(q *telemetry.Query) { q.Range = -1 },
		"step without range": func(q *telemetry.Query) { q.Step = time.Second },
		"range over max":     func(q *telemetry.Query) { q.Range = metrics.MaxRange + 1 },
	} {
		q := ok
		mut(&q)
		if err := q.Validate(); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestResultValidate(t *testing.T) {
	nan := telemetry.Result{Series: []metrics.Series{{Samples: []metrics.Sample{{Value: nanValue()}}}}}
	if err := nan.Validate(telemetry.KindMetrics); err == nil {
		t.Error("NaN sample must be rejected")
	}
	if err := (telemetry.Result{Lines: []telemetry.Line{{Text: "x"}}}).Validate(telemetry.KindMetrics); err == nil {
		t.Error("lines in a metrics result must be rejected")
	}
	if err := (telemetry.Result{Series: []metrics.Series{{}}}).Validate(telemetry.KindLogs); err == nil {
		t.Error("series in a logs result must be rejected")
	}
	long := telemetry.Result{Lines: []telemetry.Line{{Text: strings.Repeat("x", telemetry.MaxLineBytes+1)}}}
	if err := long.Validate(telemetry.KindLogs); err == nil {
		t.Error("an over-long line must be rejected")
	}
	if err := (telemetry.Result{}).Validate(telemetry.KindTraces); err == nil {
		t.Error("traces have no result contract yet")
	}
	if err := (telemetry.Result{}).Validate(telemetry.KindMetrics); err != nil {
		t.Errorf("empty result is valid: %v", err)
	}
}

func nanValue() float64 {
	zero := 0.0
	return zero / zero
}

func TestResolverAlertsTagsTheSourceAndRequiresTheCapability(t *testing.T) {
	m := telemetry.KindMetrics
	withAlerts := bound("prom", true, []telemetry.Capability{telemetry.CapAlerts}, m)
	withAlerts.Client.(*telemetrytest.Fake).SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "A"}, State: "firing"}})
	plain := bound("plain", false, nil, m)
	r, err := telemetry.NewResolver([]telemetry.Bound{withAlerts, plain})
	if err != nil {
		t.Fatal(err)
	}
	alerts, err := r.Alerts(context.Background(), "prom")
	if err != nil || len(alerts) != 1 || alerts[0].Source != "prom" {
		t.Fatalf("alerts = %+v, %v; want one alert tagged with its source", alerts, err)
	}
	if _, err := r.Alerts(context.Background(), "plain"); err == nil || !strings.Contains(err.Error(), `"plain"`) {
		t.Fatalf("source without the alerts capability: %v", err)
	}
	if _, err := r.Alerts(context.Background(), "nope"); !errors.Is(err, telemetry.ErrUnbound) {
		t.Fatalf("unbound source: %v", err)
	}
}
