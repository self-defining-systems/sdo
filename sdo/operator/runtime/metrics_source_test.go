package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

// fakeRegistry registers a "prometheus" adapter whose client is fake and
// whose Open records what the binding handed it.
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

// providedDefault is an environment providing the one metrics source `default`
// with a granted credential under credentialDir.
func providedDefault(credentialDir string) telemetry.Environment {
	return telemetry.Environment{
		Version: telemetry.EnvironmentVersion, CredentialDir: credentialDir,
		Provided: []telemetry.Binding{{
			ID: "default", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics},
			Endpoint: telemetry.Endpoint{URL: "http://prom.test:9090", Credential: "metrics-token"}, Default: true,
		}},
	}
}

func TestEnvironmentProvidedSourceServesQueriesAndAlertsThroughTheRegistry(t *testing.T) {
	at := time.Unix(1_700_000_000, 0).UTC()
	fake := telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{Series: []metrics.Series{{
		Labels: map[string]string{"job": "web"}, Samples: []metrics.Sample{{At: at, Value: 4}},
	}}})
	fake.SetAlerts([]metrics.Alert{{Labels: map[string]string{"alertname": "HighErrors"}, State: "firing"}})
	credentials := t.TempDir()
	if err := os.MkdirAll(filepath.Join(credentials, "metrics-token"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credentials, "metrics-token", "token"), []byte("abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	api, err := TelemetrySources{Registry: fakeRegistry(t, fake, &endpoint, &env), Environment: providedDefault(credentials)}.open(nil)
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.URL != "http://prom.test:9090" || endpoint.Credential != "metrics-token" {
		t.Fatalf("adapter opened with %+v", endpoint)
	}
	if token, err := env.ReadCredential(endpoint.Credential); err != nil || token != "abc" {
		t.Fatalf("credential = %q, %v; want the granted token file's content", token, err)
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

const designerSourcesYAML = `version: 1
sources:
  - id: prom-extra
    kinds: [metrics]
    adapter: prometheus
    endpoint: http://prom-extra.sdo-ctl.svc:9090
`

func TestDesignerSourcesJoinTheEnvironmentOnlyWhileItIsOpen(t *testing.T) {
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	registry := fakeRegistry(t, telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("x")), &endpoint, &env)
	for name, test := range map[string]struct {
		mode  telemetry.Mode
		bound []string
	}{
		"open":   {telemetry.ModeOpen, []string{"default", "prom-extra"}},
		"unset":  {"", []string{"default", "prom-extra"}},
		"pinned": {telemetry.ModePinned, []string{"default"}},
		"floor":  {telemetry.ModeFloor, nil},
	} {
		environment := providedDefault(t.TempDir())
		environment.Mode = test.mode
		api, err := TelemetrySources{Registry: registry, Environment: environment}.open([]byte(designerSourcesYAML))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if test.bound == nil {
			if api != nil {
				t.Errorf("%s: a floor environment binds nothing, got a source", name)
			}
			continue
		}
		// Naming the other source succeeds only for a bound one; the error
		// for an unbound id lists what is bound.
		_, err = api.Query(context.Background(), metrics.Query{ID: "q", Source: "nope", Expr: "up"}, time.Unix(1_700_000_000, 0))
		if err == nil || !strings.Contains(err.Error(), "bound: "+strings.Join(test.bound, ", ")) {
			t.Errorf("%s: err = %v, want bound: %v", name, err, test.bound)
		}
	}
}

func TestTelemetrySourcesRejectWhatCannotBeBound(t *testing.T) {
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	registry := fakeRegistry(t, telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{}), &endpoint, &env)
	open := telemetry.Environment{Version: telemetry.EnvironmentVersion}
	if api, err := (TelemetrySources{Registry: registry, Environment: open}).open(nil); api != nil || err != nil {
		t.Fatalf("nothing bound = %v, %v; want no source and no error", api, err)
	}
	for name, test := range map[string]struct {
		sources TelemetrySources
		yaml    string
		want    string
	}{
		"designer source without adapters": {TelemetrySources{Environment: open}, designerSourcesYAML, "without telemetry adapters"},
		"provided source without adapters": {TelemetrySources{Environment: providedDefault(t.TempDir())}, "", "without telemetry adapters"},
		"unparsable accepted sources":      {TelemetrySources{Registry: registry, Environment: open}, "version: 9\nsources: []\n", "accepted telemetry sources"},
		"two defaults for metrics": {TelemetrySources{Registry: registry, Environment: providedDefault(t.TempDir())},
			designerSourcesYAML + "    default: true\n", "default"},
		"designer shadows provided id": {TelemetrySources{Registry: registry, Environment: providedDefault(t.TempDir())},
			strings.Replace(designerSourcesYAML, "prom-extra", "default", 1), "default"},
	} {
		if _, err := test.sources.open([]byte(test.yaml)); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: err = %v, want %q", name, err, test.want)
		}
	}
}

func TestMetricsSourceViewCarriesTheEnvironmentsDefaultAndPublishingBase(t *testing.T) {
	environment := providedDefault(t.TempDir())
	environment.MetricsPublishURL = "http://prom.test:9090/"
	got := TelemetrySources{Environment: environment}.metricsSource()
	if got.GetQueryUrl() != "http://prom.test:9090" || !got.GetAnnotatedPodScraping() || got.GetOtlpMetricsUrl() != "http://prom.test:9090/api/v1/otlp/v1/metrics" {
		t.Fatalf("metrics source = %v", got)
	}
	if empty := (TelemetrySources{}).metricsSource(); empty.GetQueryUrl() != "" || empty.GetOtlpMetricsUrl() != "" {
		t.Fatalf("an environment providing nothing offers no URL: %v", empty)
	}
}

func TestEachRevisionsTelemetrySourcesRebuildTheObserver(t *testing.T) {
	var endpoint telemetry.Endpoint
	var env telemetry.Env
	registry := fakeRegistry(t, telemetrytest.NewFake(telemetry.KindMetrics, seriesFor("x")), &endpoint, &env)
	observations := detectorObservations{Telemetry: TelemetrySources{Registry: registry, Environment: telemetry.Environment{Version: telemetry.EnvironmentVersion}}}
	detector := sourcesDetector{queries: []metrics.Query{{ID: "q", Source: "prom-extra", Expr: "up"}}}
	_, before, err := observations.compose(staticProvider{}, []sdk.Detector{detector}, nil)
	if err != nil || before == nil || before.api != nil {
		t.Fatalf("revision without bindings: observer = %v, err = %v; want an observer with no source", before, err)
	}
	if got := fetchAll(t, nil, detector)["q"].Error; !strings.Contains(got, "not configured") {
		t.Fatalf("a revision binding nothing must report the source unavailable, got %q", got)
	}
	_, after, err := observations.compose(staticProvider{}, []sdk.Detector{detector}, []byte(designerSourcesYAML))
	if err != nil || after == nil {
		t.Fatalf("revision binding prom-extra: observer = %v, err = %v", after, err)
	}
	results := fetchAll(t, after.api, detector)
	if results["q"].Error != "" {
		t.Fatalf("the accepted revision's binding must serve its detector: %+v", results["q"])
	}
}
