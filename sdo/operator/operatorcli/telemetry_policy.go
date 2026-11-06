package operatorcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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

// installTelemetryPolicy derives the broker's policy from the install's
// telemetry profile: its mode, the cluster reads it grants, the recipes it
// applies with the reads each declares, and its provided sources. The
// controller's environment document comes from the same profile, so the
// broker, the judge's check and the controller agree on which sources exist.
func installTelemetryPolicy(config install.Config, application, control string) (telemetry.Policy, error) {
	profile := config.TelemetryProfile
	policy := telemetry.Policy{Mode: profile.Mode, ControlNamespace: control, ApplicationNamespace: application, Capabilities: profile.Capabilities}
	if policy.Mode == "" {
		policy.Mode = telemetry.ModeOpen
	}
	for _, name := range profile.Recipes {
		found := false
		for _, recipe := range config.BackendRecipes {
			if recipe.Name() == name {
				policy.Recipes = append(policy.Recipes, telemetry.Recipe{Name: name, Declares: recipe.Capabilities()})
				found = true
			}
		}
		if !found {
			return telemetry.Policy{}, fmt.Errorf("telemetry profile %q applies recipe %q, which this operator does not provide", profile.Name, name)
		}
	}
	for _, source := range profile.Sources {
		provided, err := telemetryRegistry().Source(source.Binding())
		if err != nil {
			return telemetry.Policy{}, fmt.Errorf("telemetry profile %q source %q: %w", profile.Name, source.ID, err)
		}
		policy.Provided = append(policy.Provided, provided)
	}
	return policy, nil
}

// metricsSource is the incident-facing view of the environment's default
// metrics source: its query URL and the publishing base the profile offers.
func metricsSource(env telemetry.Environment) *contracts.MetricsSource {
	source := &contracts.MetricsSource{}
	for _, binding := range env.Provided {
		if binding.Default && slices.Contains(binding.Kinds, telemetry.KindMetrics) {
			source.QueryUrl = binding.Endpoint.URL
		}
	}
	if publish := env.MetricsPublishURL; publish != "" {
		source.AnnotatedPodScraping = true
		source.OtlpMetricsUrl = strings.TrimRight(publish, "/") + "/api/v1/otlp/v1/metrics"
	}
	return source
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

// writeTelemetryEnvironment writes the install's telemetry environment
// document to a private temporary file for evaluation subprocesses that read
// `--telemetry-environment`, and returns its path with a cleanup.
func writeTelemetryEnvironment(config install.Config) (string, func(), error) {
	document, err := config.TelemetryEnvironment().Marshal()
	if err != nil {
		return "", nil, fmt.Errorf("telemetry environment: %w", err)
	}
	directory, err := os.MkdirTemp("", "sdo-telemetry-")
	if err != nil {
		return "", nil, err
	}
	path := filepath.Join(directory, "environment.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		_ = os.RemoveAll(directory)
		return "", nil, err
	}
	return path, func() { _ = os.RemoveAll(directory) }, nil
}
