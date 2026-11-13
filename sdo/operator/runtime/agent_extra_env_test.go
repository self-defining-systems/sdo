package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/worker_registry"
)

// Every agent worker the controller launches carries agent.extra_env. A provider reached through it (for example
// Claude on Vertex AI) must work for the closing turn that resumes the responder's session, not only for the
// responder; the operator configuration's agent section stays authoritative over an extra entry.
func TestEveryAgentWorkerKindReceivesExtraEnvironment(t *testing.T) {
	launch := &contracts.ControllerConfig{Namespace: "shop", ControlNamespace: "sdo-system"}
	policy := settings.Default()
	policy.Agent.Model, policy.Learning.Replay = "m", "off"
	policy.Agent.ExtraEnv = []string{"PROVIDER_ROUTE=1", "PROVIDER_KEY_PATH=/sdo/credentials/key", worker_registry.ProviderEnvironment + "=other"}
	registry := telemetry.NewRegistry()
	responder := func(config *contracts.ControllerConfig) func() (map[string]string, error) {
		return func() (map[string]string, error) { return responderJobEnvironment(config, policy, registry) }
	}
	kinds := map[string]struct {
		build         func() (map[string]string, error)
		authoritative bool
	}{
		"responder Job":                       {responder(launch), true},
		"responder Job without launch config": {responder(nil), false},
		"standby pod": {func() (map[string]string, error) {
			environment, err := responderJobEnvironment(launch, policy, registry)
			return standbyWorkerEnvironment(environment), err
		}, true},
		"reflection and closing turn": {func() (map[string]string, error) { return AgentWorkerEnvironment(launch, policy, registry) }, true},
		"health refresh judge and lifecycle agents": {func() (map[string]string, error) {
			return worker_registry.AgentEnvironment(policy.Agent), nil
		}, true},
	}
	for kind, worker := range kinds {
		environment, err := worker.build()
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if environment["PROVIDER_ROUTE"] != "1" || environment["PROVIDER_KEY_PATH"] != "/sdo/credentials/key" {
			t.Errorf("%s: agent.extra_env is missing: %v", kind, environment)
		}
		if worker.authoritative && environment[worker_registry.ProviderEnvironment] != policy.Agent.Provider {
			t.Errorf("%s: %s = %q, want the configured %q over the extra entry", kind, worker_registry.ProviderEnvironment, environment[worker_registry.ProviderEnvironment], policy.Agent.Provider)
		}
	}
}
