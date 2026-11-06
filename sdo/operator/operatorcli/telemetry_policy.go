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

// legacyMetricsSourceID names the operator-provided source that stands for
// the controller's --metrics-url until the telemetry profile replaces the flag.
const legacyMetricsSourceID = "default"

// installTelemetryPolicy derives the policy from what the installer grants.
// Until the telemetry profile lands, the observability stack decides it: the
// Prometheus recipe and its pod-discovery and node-proxy reads exist exactly
// when the stack is installed. The configured metrics URL (config.MetricsURL,
// already the effective one) is the single operator-provided source `default`,
// the same binding the controller synthesizes from --metrics-url, so the
// broker, the judge's check and the controller agree on which sources exist.
func installTelemetryPolicy(config install.Config, application, control string) (telemetry.Policy, error) {
	policy := telemetry.Policy{Mode: telemetry.ModeOpen, ControlNamespace: control, ApplicationNamespace: application}
	if !config.DisableObservability {
		policy.Capabilities = []telemetry.ClusterCapability{telemetry.CapPodDiscovery, telemetry.CapNodeProxy}
		policy.Recipes = []telemetry.Recipe{{Name: "prometheus", Declares: policy.Capabilities}}
	}
	if config.MetricsURL != "" {
		source, err := telemetryRegistry().Source(telemetry.Binding{
			ID: legacyMetricsSourceID, Adapter: "prometheus", Kinds: []telemetry.Kind{telemetry.KindMetrics},
			Endpoint: telemetry.Endpoint{URL: config.MetricsURL}, Default: true, Origin: telemetry.OriginOperator,
		})
		if err != nil {
			return telemetry.Policy{}, fmt.Errorf("metrics URL as a telemetry source: %w", err)
		}
		policy.Provided = []telemetry.Source{source}
	}
	return policy, nil
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
