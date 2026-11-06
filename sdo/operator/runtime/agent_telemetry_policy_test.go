package runtime

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/telemetry"
)

// Every controller-launched agent (responders, reflection, the refresh judge)
// self-checks its memory under the policy the controller's brokers enforce, not
// the open default. The launch environment is derived once, through
// broker.ControllerTelemetry, and both agent-environment entry points carry it.
func TestAgentWorkerEnvironmentCarriesTheBrokersTelemetryPolicy(t *testing.T) {
	launch := &contracts.ControllerConfig{Namespace: "shop", ControlNamespace: "sdo-system", Replay: "off"}
	registry := telemetry.NewRegistry()
	want, err := broker.ControllerTelemetry(launch, registry)
	if err != nil {
		t.Fatal(err)
	}
	for name, build := range map[string]func() (map[string]string, error){
		"AgentWorkerEnvironment": func() (map[string]string, error) { return AgentWorkerEnvironment(launch, registry) },
		"withAgentEnvironment":   func() (map[string]string, error) { return withAgentEnvironment(map[string]string{"X": "1"}, launch, registry) },
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
	}
}
