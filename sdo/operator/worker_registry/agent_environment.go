package worker_registry

import (
	"fmt"
	"slices"
)

// ProviderEnvironment names the variable that selects a worker's agent provider.
// Workers default to Codex when it is absent, so a configured provider must
// always travel with the Job.
const ProviderEnvironment = "SDO_AGENT_PROVIDER"

// ModelEnvironments are the per-role model variables workers read. sdo/contracts/agent_env.json
// names the same set for the Python workers; a test keeps the two equal.
var ModelEnvironments = []string{"SDO_RESPONDER_MODEL", "SDO_REFLECTION_MODEL", "SDO_LIFECYCLE_MODEL", "SDO_DEPLOYMENT_MODEL"}

// EffortEnvironment names the variable that sets the reasoning effort of every
// agent worker (deployer, health judge, responder, reflection).
const EffortEnvironment = "SDO_AGENT_EFFORT"

// AgentEfforts are the efforts a worker accepts; sdo/contracts/agent_env.json
// names the same list for the Python workers; a test keeps the two equal.
var AgentEfforts = []string{"low", "medium", "high", "xhigh", "max"}

// AgentEnvironment is the provider, model and effort a controller was configured
// with, as the environment every agent worker Job reads. An unconfigured provider
// adds nothing; an unknown provider or effort is an error, so no worker runs at an
// effort other than the configured one.
func AgentEnvironment(provider, model, effort string) (map[string]string, error) {
	if effort != "" && !slices.Contains(AgentEfforts, effort) {
		return nil, fmt.Errorf("unsupported agent effort %q: must be one of %v", effort, AgentEfforts)
	}
	if provider == "" {
		if effort != "" {
			return map[string]string{EffortEnvironment: effort}, nil
		}
		return map[string]string{}, nil
	}
	if provider != "codex" && provider != "claude" {
		return nil, fmt.Errorf("unsupported responder backend %q: must be codex or claude", provider)
	}
	environment := map[string]string{ProviderEnvironment: provider}
	if effort != "" {
		environment[EffortEnvironment] = effort
	}
	if model != "" {
		for _, name := range ModelEnvironments {
			environment[name] = model
		}
	}
	return environment, nil
}
