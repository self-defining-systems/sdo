// Package telemetrysources is the single wiring point for application-telemetry
// adapters. The controller and the operator CLI import it; no other package
// names an adapter implementation.
package telemetrysources

import (
	"fmt"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/prometheus"
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
