package prometheus_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
	"sdo.dev/operator/telemetry/telemetrytest"
)

var at = time.Unix(1_700_000_000, 0).UTC()

func vector(samples ...string) string {
	return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(samples, ",") + `]}}`
}

func item(job, value string) string {
	return fmt.Sprintf(`{"metric":{"job":%q},"value":[1700000000,%q]}`, job, value)
}

func registry(t *testing.T) *telemetry.Registry {
	t.Helper()
	r := telemetry.NewRegistry()
	if err := prometheus.Register(r); err != nil {
		t.Fatal(err)
	}
	return r
}

// doer is an in-memory HTTP transport: no sockets, no listeners.
type doer func(*http.Request) (*http.Response, error)

func (d doer) Do(r *http.Request) (*http.Response, error) { return d(r) }

func reply(code int, body string) (*http.Response, error) {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// backend answers the Prometheus HTTP API as behavior b says.
func backend(t *testing.T, b telemetrytest.Behavior) doer {
	t.Helper()
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	return func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil { // as a real transport does
			return nil, err
		}
		switch b {
		case telemetrytest.BehaviorData:
			return reply(200, vector(item("web", "1")))
		case telemetrytest.BehaviorEmpty:
			return reply(200, vector())
		case telemetrytest.BehaviorServerError:
			return reply(500, `{"status":"error","errorType":"internal","error":"boom"}`)
		case telemetrytest.BehaviorMalformed:
			return reply(200, `{"status":"success","data":`)
		case telemetrytest.BehaviorTooLarge:
			items := make([]string, telemetry.MaxSeries+1)
			for i := range items {
				items[i] = item(fmt.Sprintf("job-%d", i), "1")
			}
			return reply(200, vector(items...))
		case telemetrytest.BehaviorNonFinite:
			return reply(200, vector(item("nan", "NaN"), item("inf", "+Inf"), item("web", "2")))
		case telemetrytest.BehaviorHang:
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return nil, r.Context().Err()
		}
		return nil, fmt.Errorf("unknown behavior %v", b)
	}
}

func newClient(t *testing.T, b telemetrytest.Behavior) telemetry.Client {
	t.Helper()
	c, err := registry(t).Open(telemetry.Binding{
		ID: "prom", Adapter: prometheus.ID, Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: telemetry.Endpoint{URL: "http://prom.test:9090"},
	}, telemetry.Env{HTTP: backend(t, b)})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestConformance(t *testing.T) {
	telemetrytest.TestClient(t, telemetrytest.Harness{
		Kind:  telemetry.KindMetrics,
		Query: telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"},
		Want: telemetry.Result{Series: []metrics.Series{{
			Labels: map[string]string{"job": "web"}, Samples: []metrics.Sample{{At: at, Value: 1}},
		}}},
		New:       newClient,
		HangGrace: 500 * time.Millisecond,
	}, prometheus.ID)
}

func TestNonFiniteSamplesAreDropped(t *testing.T) {
	res, err := newClient(t, telemetrytest.BehaviorNonFinite).Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"}, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 1 || res.Series[0].Labels["job"] != "web" || res.Series[0].Samples[0].Value != 2 {
		t.Fatalf("series = %+v, want only the finite web series", res.Series)
	}
}

func TestOversizedAnswerIsAnErrorNamingTheLimit(t *testing.T) {
	_, err := newClient(t, telemetrytest.BehaviorTooLarge).Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics, Text: "up"}, at)
	if err == nil || !strings.Contains(err.Error(), "narrow the expression") {
		t.Fatalf("err = %v, want a bounds error that says to narrow the expression", err)
	}
}

func TestRequestShapes(t *testing.T) {
	var seen []*http.Request
	client := prometheus.Client{
		BaseURL: "http://prom.test/",
		Token:   func() (string, error) { return " secret\n", nil },
		HTTPClient: doer(func(r *http.Request) (*http.Response, error) {
			seen = append(seen, r)
			switch r.URL.Path {
			case "/api/v1/query_range":
				return reply(200, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[[1699999940,"1"],[1700000000,"2"]]}]}}`)
			case "/api/v1/alerts":
				return reply(200, `{"status":"success","data":{"alerts":[{"labels":{"alertname":"A"},"annotations":{},"state":"firing","activeAt":"2023-11-14T22:13:20Z","value":"1"}]}}`)
			}
			return reply(200, `{"status":"success","data":{"resultType":"scalar","result":[1700000000,"3"]}}`)
		}),
	}

	res, err := client.Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics, Text: "rate(x[1m])", Range: time.Minute}, at)
	if err != nil {
		t.Fatal(err)
	}
	got := seen[0]
	if got.URL.Path != "/api/v1/query_range" || got.URL.Query().Get("step") != "1" || got.URL.Query().Get("end") != "1700000000" || got.URL.Query().Get("start") != "1699999940" {
		t.Fatalf("range request = %s", got.URL)
	}
	if got.Header.Get("Authorization") != "Bearer secret" {
		t.Fatalf("authorization = %q", got.Header.Get("Authorization"))
	}
	if len(res.Series) != 1 || len(res.Series[0].Samples) != 2 || res.Series[0].Labels == nil {
		t.Fatalf("matrix = %+v", res.Series)
	}

	res, err = client.Query(context.Background(), telemetry.Query{Kind: telemetry.KindMetrics, Text: "scalar(x)"}, at)
	if err != nil || len(res.Series) != 1 || res.Series[0].Samples[0].Value != 3 || seen[1].URL.Query().Get("time") != "1700000000" {
		t.Fatalf("scalar = %+v, %v (%s)", res, err, seen[1].URL)
	}

	alerts, err := client.Alerts(context.Background())
	if err != nil || len(alerts) != 1 || alerts[0].Name() != "A" || alerts[0].State != "firing" {
		t.Fatalf("alerts = %+v, %v", alerts, err)
	}
}

func TestRejectsWhatItCannotServe(t *testing.T) {
	client := prometheus.Client{BaseURL: "http://prom.test"}
	if _, err := client.Query(context.Background(), telemetry.Query{Kind: telemetry.KindLogs, Text: "{a=\"b\"}"}, at); err == nil {
		t.Fatal("a logs query to Prometheus must fail")
	}
	if _, err := (prometheus.Client{}).Alerts(context.Background()); err == nil {
		t.Fatal("an unconfigured address must fail")
	}
	tokenErr := errors.New("no token")
	failing := prometheus.Client{BaseURL: "http://prom.test", Token: func() (string, error) { return "", tokenErr }}
	if _, err := failing.Alerts(context.Background()); !errors.Is(err, tokenErr) {
		t.Fatalf("err = %v, want the token error", err)
	}
}

func TestAdapterDescriptorAndBindingChecks(t *testing.T) {
	r := registry(t)
	desc, ok := r.Describe(prometheus.ID)
	if !ok || len(desc.Kinds) != 1 || desc.Kinds[0] != telemetry.KindMetrics {
		t.Fatalf("descriptor = %+v", desc)
	}
	good := telemetry.Binding{ID: "prom", Adapter: prometheus.ID, Kinds: []telemetry.Kind{telemetry.KindMetrics}, Endpoint: telemetry.Endpoint{URL: "http://p.sdo-ctl.svc:9090"}}
	if err := r.Check(good); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*telemetry.Binding){
		"no url":      func(b *telemetry.Binding) { b.Endpoint.URL = "" },
		"ftp":         func(b *telemetry.Binding) { b.Endpoint.URL = "ftp://p:9090" },
		"logs kind":   func(b *telemetry.Binding) { b.Kinds = []telemetry.Kind{telemetry.KindLogs} },
		"bad options": func(b *telemetry.Binding) { b.Options = []byte(`{"tenant":"x"}`) },
	} {
		bad := good
		mutate(&bad)
		if err := r.Check(bad); err == nil {
			t.Errorf("%s: binding accepted", name)
		}
	}
	// A credential name needs an environment that grants it.
	withCred := good
	withCred.Endpoint.Credential = "metrics-token"
	if _, err := r.Open(withCred, telemetry.Env{}); err == nil {
		t.Fatal("credential without a granted environment was opened")
	}
	if _, err := r.Open(withCred, telemetry.Env{ReadCredential: func(string) (string, error) { return "t", nil }}); err != nil {
		t.Fatal(err)
	}
}
