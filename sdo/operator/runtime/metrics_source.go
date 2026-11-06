package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/telemetry"
)

const (
	// flagSourceID names the one source the --metrics-url flag binds.
	flagSourceID = "default"
	// flagCredential names the credential --metrics-token-file grants.
	flagCredential = "metrics-token"
	// flagAdapter is the adapter the --metrics-url flag selects.
	flagAdapter = "prometheus"
)

// newMetricsSource opens the Prometheus-compatible source at address through
// the telemetry registry, or returns nil when none is configured. The
// registry is wired by the entry point (operatorcli/catalog/telemetrysources);
// this package names no adapter implementation.
func newMetricsSource(registry *telemetry.Registry, address string, tokenFile string) (MetricsAPI, error) {
	if address == "" {
		if tokenFile != "" {
			return nil, errors.New("--metrics-token-file requires --metrics-url")
		}
		return nil, nil
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("--metrics-url %q must be an http or https URL", address)
	}
	if registry == nil {
		return nil, errors.New("--metrics-url needs a telemetry registry: this controller was built without telemetry adapters")
	}
	binding := telemetry.Binding{
		ID: flagSourceID, Adapter: flagAdapter, Kinds: []telemetry.Kind{telemetry.KindMetrics},
		Endpoint: telemetry.Endpoint{URL: address}, Default: true, Origin: telemetry.OriginOperator,
	}
	var env telemetry.Env
	if tokenFile != "" {
		binding.Endpoint.Credential = flagCredential
		env.ReadCredential = func(string) (string, error) {
			token, err := os.ReadFile(tokenFile)
			return string(token), err
		}
	}
	client, err := registry.Open(binding, env)
	if err != nil {
		return nil, err
	}
	return telemetryMetrics{client: client}, nil
}

// telemetryMetrics adapts a telemetry.Client to the observer's MetricsAPI.
type telemetryMetrics struct{ client telemetry.Client }

func (m telemetryMetrics) Query(ctx context.Context, query metrics.Query, at time.Time) ([]metrics.Series, error) {
	request := telemetry.Query{SourceID: flagSourceID, Kind: telemetry.KindMetrics, Text: query.Expr}
	if query.Range > 0 {
		request.Range, request.Step = query.Range, query.EffectiveStep()
	}
	result, err := m.client.Query(ctx, request, at)
	if err != nil {
		return nil, err
	}
	if result.Truncated {
		return nil, errors.New("source truncated the answer; narrow the expression")
	}
	return result.Series, nil
}

func (m telemetryMetrics) Alerts(ctx context.Context) ([]metrics.Alert, error) {
	reader, ok := m.client.(telemetry.AlertReader)
	if !ok {
		return nil, errors.New("source does not serve alerts")
	}
	return reader.Alerts(ctx)
}

// withMetrics adds frozen metric results to provider's snapshots when a
// detector consumes metrics or alerts. With requireSource, such detectors
// without an address are an error; otherwise they run unconfigured and report
// their source as unavailable on every evaluation.
func withMetrics(
	provider SnapshotProvider, detectors []sdk.Detector, registry *telemetry.Registry, address string, tokenFile string, requireSource bool, clock func() time.Time,
) (SnapshotProvider, *MetricsObserver, error) {
	api, err := newMetricsSource(registry, address, tokenFile)
	if err != nil {
		return nil, nil, err
	}
	return withMetricsAPI(provider, detectors, api, requireSource, clock)
}

// withMetricsAPI is withMetrics for an already constructed metrics source.
func withMetricsAPI(
	provider SnapshotProvider, detectors []sdk.Detector, api MetricsAPI, requireSource bool, clock func() time.Time,
) (SnapshotProvider, *MetricsObserver, error) {
	observer, err := NewMetricsObserver(api, detectors, MetricsObserverOptions{Clock: clock})
	if err != nil {
		return nil, nil, err
	}
	if observer == nil {
		if api == nil {
			return provider, nil, nil
		}
		// Configured sources also serve on-demand Diagnose queries.
		observer = &MetricsObserver{api: api, timeout: DefaultMetricsQueryTimeout, clock: lifecycleio.Now}
	}
	if api == nil && requireSource {
		return nil, nil, errors.New("metrics health evaluation requires --metrics-url")
	}
	return MetricsSnapshotProvider{Base: provider, Observer: observer}, observer, nil
}
