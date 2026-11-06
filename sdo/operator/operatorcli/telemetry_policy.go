package operatorcli

import (
	"encoding/json"
	"fmt"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetry"
)

// memoryTelemetryPolicyEnv carries the broker's telemetry policy (JSON) to the
// agents' `memory check`, so a self-check and the broker apply the same rules.
const memoryTelemetryPolicyEnv = "SDO_TELEMETRY_POLICY"

// telemetryRegistry is the adapter registry bindings are checked against: the
// same catalog wiring the controller opens clients from.
func telemetryRegistry() *telemetry.Registry { return telemetrysources.Registry() }

// installTelemetryPolicy derives the policy from what the installer grants.
// Until the telemetry profile lands, the observability stack decides it: the
// Prometheus recipe and its pod-discovery and node-proxy reads exist exactly
// when the stack is installed. No source is operator-provided yet.
func installTelemetryPolicy(config install.Config, application, control string) telemetry.Policy {
	policy := telemetry.Policy{Mode: telemetry.ModeOpen, ControlNamespace: control, ApplicationNamespace: application}
	if !config.DisableObservability {
		policy.Capabilities = []telemetry.ClusterCapability{telemetry.CapPodDiscovery, telemetry.CapNodeProxy}
		policy.Recipes = []telemetry.Recipe{{Name: "prometheus", Declares: policy.Capabilities}}
	}
	return policy
}

// memoryTelemetryPolicy reads the policy an agent check runs under. With no
// environment value the policy is the default open mode with only the
// application namespace granted, which is what a local self-check can know.
func memoryTelemetryPolicy(getenv func(string) string, application string) (telemetry.Policy, error) {
	raw := getenv(memoryTelemetryPolicyEnv)
	if raw == "" {
		return telemetry.Policy{ApplicationNamespace: application}, nil
	}
	var policy telemetry.Policy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return telemetry.Policy{}, fmt.Errorf("%s: %w", memoryTelemetryPolicyEnv, err)
	}
	if err := policy.Validate(); err != nil {
		return telemetry.Policy{}, fmt.Errorf("%s: %w", memoryTelemetryPolicyEnv, err)
	}
	return policy, nil
}
