package runtime

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/worker_registry"
)

// Every controller-launched agent (responders, reflection, the refresh judge)
// self-checks its memory under the policy the controller's brokers enforce, not
// the open default. The launch environment is derived once, through
// broker.ControllerTelemetry, and both agent-environment entry points carry it.
func TestAgentWorkerEnvironmentCarriesTheBrokersTelemetryPolicy(t *testing.T) {
	launch := &contracts.ControllerConfig{Namespace: "shop", ControlNamespace: "sdo-system"}
	policy := settings.Default()
	policy.Agent.Model, policy.Learning.Replay = "m", "off"
	registry := telemetry.NewRegistry()
	want, err := broker.ControllerTelemetry(launch, registry)
	if err != nil {
		t.Fatal(err)
	}
	for name, build := range map[string]func() (map[string]string, error){
		"AgentWorkerEnvironment": func() (map[string]string, error) { return AgentWorkerEnvironment(launch, policy, registry) },
		"withAgentEnvironment": func() (map[string]string, error) {
			return withAgentEnvironment(map[string]string{"X": "1"}, launch, policy, registry)
		},
	} {
		environment, err := build()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var got telemetry.Policy
		if err := json.Unmarshal([]byte(environment[broker.TelemetryPolicyEnvironment]), &got); err != nil {
			t.Fatalf("%s: policy is not carried: %v", name, err)
		}
		if got.ControlNamespace != want.Policy.ControlNamespace || got.ApplicationNamespace != "shop" || got.ControlNamespace != "sdo-system" {
			t.Errorf("%s: policy = %+v, want the broker's %+v", name, got, want.Policy)
		}
		// The operator configuration's agent section and replay travel with every worker, defaults included.
		for variable, value := range map[string]string{
			worker_registry.ProviderEnvironment: "codex", worker_registry.EffortEnvironment: "medium", "SDO_RESPONDER_MODEL": "m",
			worker_registry.LifecycleTimeoutEnvironment: "3600", worker_registry.MemoryCheckTimeoutEnvironment: "300", settings.ReplayEnvironment: "off",
		} {
			if environment[variable] != value {
				t.Errorf("%s: %s = %q, want %q", name, variable, environment[variable], value)
			}
		}
	}
}
