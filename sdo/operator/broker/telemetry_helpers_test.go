package broker

import "sdo.dev/operator/telemetry"

// testObservation is a broker policy with an empty telemetry registry: no
// adapter, so a tree that binds a source is rejected as unregistered.
func testObservation() ObservationPolicy {
	return ObservationPolicy{SettingsDigest: testSettingsDigest, Telemetry: Telemetry{Registry: telemetry.NewRegistry()}}
}

// testSettingsDigest stands for settings.Config.DetectionDigest in tests: the broker binds operability reports to it.
const testSettingsDigest = "settings-digest"
