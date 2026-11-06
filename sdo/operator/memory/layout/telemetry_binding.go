package layout

import (
	"encoding/json"
	"fmt"

	"sdo.dev/operator/telemetry"
)

// Binding converts a stored designer source to the registry's binding with
// the given (already validated) kinds. Options that are not JSON-compatible
// are the caller's diagnostic.
func (s TelemetrySource) Binding(kinds []telemetry.Kind) (telemetry.Binding, error) {
	options, err := json.Marshal(s.Options)
	if err != nil {
		return telemetry.Binding{}, fmt.Errorf("source %q options are not JSON-compatible: %w", s.ID, err)
	}
	if len(s.Options) == 0 {
		options = nil
	}
	origin := telemetry.OriginAppShipped
	if s.Backend != "" {
		origin = telemetry.OriginDesignerBackend
	}
	return telemetry.Binding{ID: s.ID, Adapter: s.Adapter, Kinds: kinds, Endpoint: telemetry.Endpoint{URL: s.Endpoint, Credential: s.Credential}, Default: s.Default, Origin: origin, Options: options}, nil
}

// TelemetryBindings parses the content of TelemetrySourcesFile into the
// bindings it declares. Empty data declares none. It checks the stored form
// only; adapter, endpoint and credential rules belong to the shared check.
func TelemetryBindings(data []byte) ([]telemetry.Binding, error) {
	if len(data) == 0 {
		return nil, nil
	}
	parsed, err := ParseTelemetrySources(data)
	if err != nil {
		return nil, err
	}
	out := make([]telemetry.Binding, 0, len(parsed.Sources))
	for _, src := range parsed.Sources {
		kinds := make([]telemetry.Kind, len(src.Kinds))
		for i, k := range src.Kinds {
			kinds[i] = telemetry.Kind(k)
		}
		binding, err := src.Binding(kinds)
		if err != nil {
			return nil, err
		}
		out = append(out, binding)
	}
	return out, nil
}
