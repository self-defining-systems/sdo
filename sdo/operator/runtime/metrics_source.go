package runtime

import (
	"errors"
	"fmt"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetrycontext"
)

// TelemetrySources builds the observer's metrics source for one accepted
// memory revision: the operator-provided bindings of the environment plus,
// while the environment is open, the designer's bindings the memory host
// reports. The registry is wired by the entry point
// (operatorcli/catalog/telemetrysources); this package names no adapter
// implementation.
type TelemetrySources struct {
	Registry    *telemetry.Registry
	Environment telemetry.Environment
}

// loadTelemetrySources reads the environment document at path; an empty path
// is an environment that provides nothing.
func loadTelemetrySources(registry *telemetry.Registry, path string) (TelemetrySources, error) {
	if path == "" {
		return TelemetrySources{Registry: registry, Environment: telemetry.Environment{Version: telemetry.EnvironmentVersion}}, nil
	}
	env, err := telemetry.LoadEnvironment(path)
	if err != nil {
		return TelemetrySources{}, err
	}
	return TelemetrySources{Registry: registry, Environment: env}, nil
}

// open returns the metrics source for the revision whose host reported
// designerSources (the accepted sources.yaml), or nil when no source is bound
// at all, so a detector that needs one reports it unavailable instead of
// healthy. A binding the registry rejects, a duplicate id or two defaults for
// a kind fail the whole revision.
func (t TelemetrySources) open(designerSources []byte) (MetricsAPI, error) {
	bindings := t.Environment.ProvidedBindings()
	if t.Environment.EffectiveMode() == telemetry.ModeOpen {
		designer, err := layout.TelemetryBindings(designerSources)
		if err != nil {
			return nil, fmt.Errorf("accepted telemetry sources: %w", err)
		}
		// The broker checked these at publication; the controller checks them
		// again against the environment's own grants before it opens them.
		policy := t.Environment.Policy()
		for _, binding := range designer {
			if err := policy.CheckBinding(binding).Err(); err != nil {
				return nil, fmt.Errorf("telemetry source %q: %w", binding.ID, err)
			}
		}
		bindings = append(bindings, designer...)
	}
	if len(bindings) == 0 {
		return nil, nil
	}
	if t.Registry == nil {
		return nil, errors.New("telemetry sources are bound but this controller was built without telemetry adapters")
	}
	env := telemetry.Env{ReadCredential: t.Environment.ReadCredential}
	bound := make([]telemetry.Bound, 0, len(bindings))
	for _, binding := range bindings {
		client, err := t.Registry.Open(binding, env)
		if err != nil {
			return nil, fmt.Errorf("telemetry source %q: %w", binding.ID, err)
		}
		source, err := t.Registry.Source(binding)
		if err != nil {
			return nil, fmt.Errorf("telemetry source %q: %w", binding.ID, err)
		}
		bound = append(bound, telemetry.Bound{Source: source, Client: client})
	}
	resolver, err := telemetry.NewResolver(bound)
	if err != nil {
		return nil, err
	}
	return newResolverMetrics(resolver), nil
}

// withMetrics adds frozen metric results to provider's snapshots when a
// detector consumes metrics or alerts. With requireSource, such detectors
// without an address are an error; otherwise they run unconfigured and report
// their source as unavailable on every evaluation.
func withMetrics(
	provider SnapshotProvider, detectors []sdk.Detector, sources TelemetrySources, designerSources []byte, requireSource bool, clock func() time.Time,
) (SnapshotProvider, *MetricsObserver, error) {
	api, err := sources.open(designerSources)
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
		return nil, nil, errors.New("metrics health evaluation requires a bound telemetry source (the environment provides none and the accepted revision binds none)")
	}
	return MetricsSnapshotProvider{Base: provider, Observer: observer}, observer, nil
}

// context is the task-facing telemetry view for the revision whose host
// reported designerSources.
func (t TelemetrySources) context(designerSources []byte) (*contractsv1alpha1.TelemetryContext, error) {
	return telemetrycontext.Build(t.Registry, t.Environment, designerSources)
}
