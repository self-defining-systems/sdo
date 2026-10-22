package worker_registry

import "fmt"

// ProviderEnvironment names the variable that selects a worker's agent provider.
// Workers default to Codex when it is absent, so a configured provider must
// always travel with the Job.
const ProviderEnvironment = "SDO_AGENT_PROVIDER"

// ModelEnvironments are the per-role model variables workers read. sdo/contracts/agent_env.json
// names the same set for the Python workers; a test keeps the two equal.
var ModelEnvironments = []string{"SDO_RESPONDER_MODEL", "SDO_REFLECTION_MODEL", "SDO_LIFECYCLE_MODEL", "SDO_DEPLOYMENT_MODEL"}

// AgentEnvironment is the provider and model a controller was configured with,
// as the environment every agent worker Job reads. An unconfigured provider adds
// nothing; an unknown one is an error.
func AgentEnvironment(provider, model string) (map[string]string, error) {
	if provider == "" {
		return map[string]string{}, nil
	}
	if provider != "codex" && provider != "claude" {
		return nil, fmt.Errorf("unsupported responder backend %q: must be codex or claude", provider)
	}
	environment := map[string]string{ProviderEnvironment: provider}
	if model != "" {
		for _, name := range ModelEnvironments {
			environment[name] = model
		}
	}
	return environment, nil
}
