// Package telemetrysources is the single wiring point for application-telemetry
// adapters. The controller and the operator CLI import it; no other package
// names an adapter implementation.
package telemetrysources

import (
	"fmt"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
	"sdo.dev/operator/telemetry/prometheus/recipe"
)

// Registry returns a registry holding every built-in adapter: prometheus.
// Registration of a built-in cannot fail, so a failure is a programming error
// and panics.
func Registry() *telemetry.Registry {
	r := telemetry.NewRegistry()
	for _, register := range []func(*telemetry.Registry) error{prometheus.Register} {
		if err := register(r); err != nil {
			panic(fmt.Sprintf("telemetrysources: register built-in adapter: %v", err))
		}
	}
	return r
}

// Recipes returns the SDO-owned backend recipes the installer may apply:
// prometheus.
func Recipes() []telemetry.BackendRecipe {
	return []telemetry.BackendRecipe{recipe.New()}
}

// Built-in profile names. DefaultProfileName is what an operator gets without
// --telemetry-profile.
const (
	DefaultProfileName = "prometheus-default"
	NoneProfileName    = "none"
)

// BuiltinProfile returns a named built-in profile for the control namespace:
// prometheus-default applies the SDO Prometheus recipe and provides it as the
// default source `prom`, with the pod-discovery and node-proxy reads and
// OTLP publishing; none installs and provides nothing.
func BuiltinProfile(name, controlNamespace string) (telemetry.Profile, bool) {
	switch name {
	case DefaultProfileName:
		return telemetry.Profile{
			Name: name, Mode: telemetry.ModeOpen,
			Recipes:      []string{recipe.RecipeName},
			Capabilities: recipe.New().Capabilities(),
			Sources: []telemetry.ProfileSource{{
				ID: "prom", Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics},
				Endpoint: recipe.QueryURL(controlNamespace), Default: true,
			}},
			MetricsPublishURL: recipe.QueryURL(controlNamespace),
		}, true
	case NoneProfileName:
		return telemetry.Profile{Name: name, Mode: telemetry.ModeOpen}, true
	}
	return telemetry.Profile{}, false
}
