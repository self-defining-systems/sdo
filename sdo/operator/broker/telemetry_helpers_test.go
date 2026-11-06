package broker

import "sdo.dev/operator/telemetry"

// testObservation is a broker policy with an empty telemetry registry: no
// adapter, so a tree that binds a source is rejected as unregistered.
func testObservation() ObservationPolicy {
	return ObservationPolicy{Telemetry: Telemetry{Registry: telemetry.NewRegistry()}}
}
