package operatorcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
)

// memoryTelemetryPolicyEnv carries the broker's telemetry policy (JSON) to the
// agents' `memory check`, so a self-check and the broker apply the same rules.
const memoryTelemetryPolicyEnv = broker.TelemetryPolicyEnvironment

// telemetryRegistry is the adapter registry bindings are checked against: the
// same catalog wiring the controller opens clients from.
func telemetryRegistry() *telemetry.Registry { return telemetrysources.Registry() }

// installTelemetryPolicy derives the broker's policy from the install's
// telemetry profile with the operator's adapter registry; see installTelemetry.
func installTelemetryPolicy(config install.Config, application, control string) (telemetry.Policy, error) {
	derived, err := installTelemetry(config, telemetryRegistry(), application, control)
	return derived.Policy, err
}

// installTelemetry derives a broker's telemetry from the install's profile
// through the same environment document the controller reads
// (broker.TelemetryFromEnvironment), so the onboarding broker, the judge's
// check and the controller's brokers agree on sources, grants and recipes.
func installTelemetry(config install.Config, registry *telemetry.Registry, application, control string) (broker.Telemetry, error) {
	profile := config.TelemetryProfile
	for _, name := range profile.Recipes {
		if !slices.ContainsFunc(config.BackendRecipes, func(r telemetry.BackendRecipe) bool { return r.Name() == name }) {
			return broker.Telemetry{}, fmt.Errorf("telemetry profile %q applies recipe %q, which this operator does not provide", profile.Name, name)
		}
	}
	config.Namespace, config.ControlNamespace = application, control
	derived, err := broker.TelemetryFromEnvironment(config.TelemetryEnvironment(), registry)
	if err != nil {
		return broker.Telemetry{}, fmt.Errorf("telemetry profile %q: %w", profile.Name, err)
	}
	return derived, nil
}

// onboardingBroker is the broker of `operate`'s lifecycle (deployer, health
// judge, verification): the accepted repository with its validator, the
// observation ceiling and the install's telemetry policy over registry. The
// judge's own check receives the same policy (SDO_TELEMETRY_POLICY) and the
// validation subprocess the same policy and the composition root's registry.
func onboardingBroker(config OperateConfig, control string, validator broker.Validator, ceiling sdk.ObservationScope, registry *telemetry.Registry) (*broker.Broker, error) {
	derived, err := installTelemetry(config.Install, registry, config.Lifecycle.Namespace, control)
	if err != nil {
		return nil, err
	}
	repository, err := broker.New(config.ApplicationRoot, config.WorktreeRoot, validator, broker.ObservationPolicy{
		Telemetry: derived,
		Replay:    config.Install.Replay, CodePath: memoryCodePath(config.Install.CodePath), Ceiling: ceiling, ApplicationNamespace: config.Lifecycle.Namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("open accepted repository broker: %w", err)
	}
	repository.PlanCatalogs = planCatalogs()
	return repository, nil
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
