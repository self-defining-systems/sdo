package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

// fakeRegistry registers a "prometheus" adapter whose client is fake and
// whose Open records what the flag-synthesized binding handed it.
func fakeRegistry(t *testing.T, fake *telemetrytest.Fake, opened *telemetry.Endpoint, env *telemetry.Env) *telemetry.Registry {
	t.Helper()
	registry := telemetry.NewRegistry()
	err := telemetry.Register(registry, telemetry.Adapter[struct{}]{
		Name: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Dialect: "PromQL",
		Caps:     []telemetry.Capability{telemetry.CapAlerts, telemetry.CapRange},
		Validate: func(struct{}, telemetry.Endpoint) error { return nil },
		Open: func(_ struct{}, endpoint telemetry.Endpoint, e telemetry.Env) (telemetry.Client, error) {
			*opened, *env = endpoint, e
			return fake, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestMetricsSourceFlagsBindOneDefaultSourceThroughTheRegistry(t *testing.T) {
	at := time.Unix(1_700_000_000, 0).UTC()
	fake := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{Series: []metrics.Series{{
		Labels: map[string]string{"job": "web"}, Samples: []metrics.Sample{{At: at, Value: 4}},
	}}})
	fake.SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "HighErrors"}, State: "firing"}})
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	api, err := newMetricsSource(fakeRegistry(t, fake, &endpoint, &env), "http://prom.test:9090", tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.URL != "http://prom.test:9090" || endpoint.Credential == "" {
		t.Fatalf("adapter opened with %+v", endpoint)
	}
	if token, err := env.ReadCredential(endpoint.Credential); err != nil || strings.TrimSpace(token) != "abc" {
		t.Fatalf("credential = %q, %v; want the token file's content", token, err)
	}

	series, err := api.Query(context.Background(), metrics.Query{ID: "q", Expr: "rate(x[1m])", Range: time.Minute}, at)
	if err != nil || len(series) != 1 || series[0].Samples[0].Value != 4 {
		t.Fatalf("series = %+v, err = %v", series, err)
	}
	calls := fake.Calls()
	if len(calls) != 1 || calls[0].Text != "rate(x[1m])" || calls[0].Kind != telemetry.KindMetrics || calls[0].Range != time.Minute || calls[0].Step != time.Second {
		t.Fatalf("telemetry query = %+v; the query text passes through unparsed and the range query gets its effective step", calls)
	}
	if _, err := api.Query(context.Background(), metrics.Query{ID: "i", Expr: "up"}, at); err != nil {
		t.Fatalf("instant query: %v", err)
	}
	if last := fake.Calls()[1]; last.Range != 0 || last.Step != 0 {
		t.Fatalf("instant query carries range/step: %+v", last)
	}
	alerts, err := api.Alerts(context.Background())
	if err != nil || len(alerts) != 1 || alerts[0].Name() != "HighErrors" {
		t.Fatalf("alerts = %+v, err = %v", alerts, err)
	}

	fake.Set(telemetrytest.BehaviorServerError)
	if _, err := api.Query(context.Background(), metrics.Query{ID: "q", Expr: "up"}, at); err == nil {
		t.Fatal("a source error must surface as a query error, never an empty result")
	}
}

func TestMetricsSourceFlagValidation(t *testing.T) {
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	registry := fakeRegistry(t, telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{}), &endpoint, &env)
	if api, err := newMetricsSource(registry, "", ""); api != nil || err != nil {
		t.Fatalf("no flags = %v, %v; want no source and no error", api, err)
	}
	for name, test := range map[string]struct {
		registry       *telemetry.Registry
		address, token string
		want           string
	}{
		"token alone": {registry, "", "/t", "--metrics-token-file requires --metrics-url"},
		"bad scheme":  {registry, "ftp://x", "", "must be an http or https URL"},
		"no registry": {nil, "http://x:9090", "", "telemetry registry"},
	} {
		if _, err := newMetricsSource(test.registry, test.address, test.token); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: err = %v, want %q", name, err, test.want)
		}
	}
}
