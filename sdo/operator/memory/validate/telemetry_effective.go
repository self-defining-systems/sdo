package validate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

// telemetryBinding converts a stored designer source to the registry's binding.
func telemetryBinding(src layout.TelemetrySource, kinds []telemetry.Kind) (telemetry.Binding, error) {
	return src.Binding(kinds)
}

// EffectiveTelemetrySources is the source set detectors resolve against for
// the tree at root: the operator-provided sources of the policy plus, in open
// mode, the designer's bound sources. A pinned environment admits only its
// provided sources and a floor environment none. The result is never nil, so a
// caller can tell "no sources" from "not computed". Callers run it after the
// static rules passed; a binding the registry rejects is an error here too.
func EffectiveTelemetrySources(root string, policy telemetry.Policy, registry *telemetry.Registry) ([]telemetry.Source, error) {
	out := []telemetry.Source{}
	mode := policy.EffectiveMode()
	if mode == telemetry.ModeFloor {
		return out, nil
	}
	for _, src := range policy.Provided {
		if src.Origin == "" {
			src.Origin = telemetry.OriginOperator
		}
		out = append(out, src)
	}
	if mode != telemetry.ModeOpen {
		return out, nil
	}
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(layout.TelemetrySourcesFile)))
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read telemetry sources: %w", err)
	}
	parsed, err := layout.ParseTelemetrySources(data)
	if err != nil {
		return nil, err
	}
	if registry == nil && len(parsed.Sources) > 0 {
		return nil, errors.New("telemetry sources are bound but no adapter registry is configured")
	}
	for _, src := range parsed.Sources {
		kinds := make([]telemetry.Kind, len(src.Kinds))
		for i, k := range src.Kinds {
			kinds[i] = telemetry.Kind(k)
		}
		binding, err := telemetryBinding(src, kinds)
		if err != nil {
			return nil, err
		}
		source, err := registry.Source(binding)
		if err != nil {
			return nil, err
		}
		out = append(out, source)
	}
	return out, nil
}

// HasAlertSource reports whether any effective source serves alerts. It is the
// condition for installing the generic alerts detector.
func HasAlertSource(sources []telemetry.Source) bool {
	return slices.ContainsFunc(sources, func(s telemetry.Source) bool { return slices.Contains(s.Capabilities, telemetry.CapAlerts) })
}
