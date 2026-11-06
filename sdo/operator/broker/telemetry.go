package broker

import (
	"encoding/json"
	"errors"
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/telemetry"
)

// Telemetry is the environment's application telemetry a broker checks
// designer bindings and detector queries against: the policy (mode, grants,
// operator-provided sources) and the adapter registry. Any accepted tree may
// hold bindings, so every broker needs both, and its validation subprocess
// receives the same policy. New refuses a broker without a registry instead of
// letting it reject bindings at publication.
type Telemetry struct {
	Policy   telemetry.Policy
	Registry *telemetry.Registry
}

func (t Telemetry) validate() error {
	if t.Registry == nil {
		return errors.New("broker requires the telemetry adapter registry (ObservationPolicy.Telemetry)")
	}
	if err := t.Policy.Validate(); err != nil {
		return fmt.Errorf("broker telemetry policy: %w", err)
	}
	return nil
}

// TelemetryPolicyEnvironment names the environment variable that carries a
// broker's telemetry policy (JSON) to an agent worker's `memory check`, so the
// agent's self-check and the broker apply the same rules.
const TelemetryPolicyEnvironment = "SDO_TELEMETRY_POLICY"

// PolicyEnvironment is the agent-worker environment entry carrying this
// broker's policy; every launch of an agent that edits memory merges it.
func (b *Broker) PolicyEnvironment() (map[string]string, error) {
	return policyEnvironment(b.telemetry.Policy)
}

// ControllerPolicyEnvironment is PolicyEnvironment for a launch whose broker is
// not at hand: the same derivation (ControllerTelemetry) the controller's
// brokers use.
func ControllerPolicyEnvironment(config *contracts.ControllerConfig, registry *telemetry.Registry) (map[string]string, error) {
	derived, err := ControllerTelemetry(config, registry)
	if err != nil {
		return nil, err
	}
	return policyEnvironment(derived.Policy)
}

func policyEnvironment(policy telemetry.Policy) (map[string]string, error) {
	encoded, err := json.Marshal(policy)
	if err != nil {
		return nil, fmt.Errorf("encode telemetry policy: %w", err)
	}
	return map[string]string{TelemetryPolicyEnvironment: string(encoded)}, nil
}

// TelemetryPolicy is the policy every check of this broker runs under; agent
// self-checks must receive the same value.
func (b *Broker) TelemetryPolicy() telemetry.Policy { return b.telemetry.Policy }

// TelemetryFromEnvironment is the one derivation of a broker's telemetry from
// the operator's telemetry environment document, which the install writes from
// its profile and the controller reads at start: the environment's binding
// policy (mode, granted namespaces, hosts, credentials, cluster reads, offered
// recipes) plus its provided sources as the registry describes them. The
// onboarding broker and every controller broker call it, so a tree is checked
// under the same policy wherever it is published.
func TelemetryFromEnvironment(env telemetry.Environment, registry *telemetry.Registry) (Telemetry, error) {
	if registry == nil {
		return Telemetry{}, errors.New("broker telemetry requires the adapter registry")
	}
	if err := env.Validate(); err != nil {
		return Telemetry{}, err
	}
	policy := env.Policy()
	policy.Mode = env.EffectiveMode()
	for _, binding := range env.ProvidedBindings() {
		source, err := registry.Source(binding)
		if err != nil {
			return Telemetry{}, fmt.Errorf("telemetry environment source %q: %w", binding.ID, err)
		}
		policy.Provided = append(policy.Provided, source)
	}
	result := Telemetry{Policy: policy, Registry: registry}
	if err := result.validate(); err != nil {
		return Telemetry{}, err
	}
	return result, nil
}

// ControllerTelemetry is a controller broker's telemetry: its launch's
// environment document. A launch without one is an open environment that
// provides nothing and grants only the launch's namespaces.
func ControllerTelemetry(config *contracts.ControllerConfig, registry *telemetry.Registry) (Telemetry, error) {
	env := telemetry.Environment{Version: telemetry.EnvironmentVersion}
	if path := config.GetTelemetryEnvironmentFile(); path != "" {
		var err error
		if env, err = telemetry.LoadEnvironment(path); err != nil {
			return Telemetry{}, err
		}
	} else {
		env.ApplicationNamespace, env.ControlNamespace = config.GetNamespace(), config.GetControlNamespace()
		if env.ControlNamespace == "" {
			env.ControlNamespace = env.ApplicationNamespace
		}
	}
	return TelemetryFromEnvironment(env, registry)
}
