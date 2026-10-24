package runtime

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"sdo.dev/operator/runtime/prometheus"
	"sdo.dev/operator/sdk"
)

// newMetricsSource returns the Prometheus-compatible source at address, or
// nil when none is configured.
func newMetricsSource(address string, tokenFile string) (MetricsAPI, error) {
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
	return prometheus.Client{BaseURL: address, TokenFile: tokenFile}, nil
}

// withMetrics adds frozen metric results to provider's snapshots when a
// detector consumes metrics or alerts. With requireSource, such detectors
// without an address are an error; otherwise they run unconfigured and report
// their source as unavailable on every evaluation.
func withMetrics(
	provider SnapshotProvider, detectors []sdk.Detector, address string, tokenFile string, requireSource bool,
) (SnapshotProvider, *MetricsObserver, error) {
	api, err := newMetricsSource(address, tokenFile)
	if err != nil {
		return nil, nil, err
	}
	observer, err := NewMetricsObserver(api, detectors, MetricsObserverOptions{})
	if err != nil {
		return nil, nil, err
	}
	if observer == nil {
		if api == nil {
			return provider, nil, nil
		}
		// Configured sources also serve on-demand Diagnose queries.
		observer = &MetricsObserver{api: api, timeout: DefaultMetricsQueryTimeout, clock: time.Now}
	}
	if api == nil && requireSource {
		return nil, nil, errors.New("metrics health evaluation requires --metrics-url")
	}
	return MetricsSnapshotProvider{Base: provider, Observer: observer}, observer, nil
}
