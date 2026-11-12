package worker_registry

import (
	"strconv"
	"time"

	"sdo.dev/operator/contracts/settings"
)

// ProviderEnvironment names the variable that selects a worker's agent provider.
const ProviderEnvironment = "SDO_AGENT_PROVIDER"

// ModelEnvironments are the per-role model variables workers read. sdo/contracts/agent_env.json
// names the same set for the Python workers; a test keeps the two equal.
var ModelEnvironments = []string{"SDO_RESPONDER_MODEL", "SDO_REFLECTION_MODEL", "SDO_LIFECYCLE_MODEL", "SDO_DEPLOYMENT_MODEL"}

// EffortEnvironment names the variable that sets the reasoning effort of every
// agent worker (deployer, health judge, responder, reflection).
const EffortEnvironment = "SDO_AGENT_EFFORT"

// AgentEfforts are the efforts a worker accepts; sdo/contracts/agent_env.json
// names the same list for the Python workers; a test keeps the two equal.
var AgentEfforts = settings.AgentEfforts

// LifecycleTimeoutEnvironment names the variable that bounds one lifecycle
// (detection designer) session, in whole seconds.
const LifecycleTimeoutEnvironment = settings.LifecycleTimeoutEnvironment

// MemoryCheckTimeoutEnvironment names the variable that bounds one memory check a worker runs, in whole seconds.
const MemoryCheckTimeoutEnvironment = settings.MemoryCheckTimeoutEnvironment

// AgentEnvironment is the agent section of the resolved operator configuration as the environment every agent
// worker Job reads: provider, model, effort and both limits are always set, so no worker falls back to a default of
// its own. The section was validated when the configuration was read.
func AgentEnvironment(agent settings.Agent) map[string]string {
	environment := ProviderCapacityEnvironment()
	environment[ProviderEnvironment] = agent.Provider
	for _, name := range ModelEnvironments {
		environment[name] = agent.Model
	}
	environment[EffortEnvironment] = agent.Effort
	environment[LifecycleTimeoutEnvironment] = wholeSeconds(time.Duration(agent.LifecycleTimeout))
	environment[MemoryCheckTimeoutEnvironment] = wholeSeconds(time.Duration(agent.MemoryCheckTimeout))
	return environment
}

// wholeSeconds rounds a limit up, so a worker never runs shorter than configured.
func wholeSeconds(limit time.Duration) string {
	seconds := limit / time.Second
	if limit%time.Second > 0 {
		seconds++
	}
	return strconv.FormatInt(int64(seconds), 10)
}
