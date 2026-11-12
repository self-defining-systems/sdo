package worker_registry

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	"sdo.dev/operator/contracts/settings"
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

// ReasoningSummaryEnvironment names the variable carrying the Codex reasoning summary mode.
const ReasoningSummaryEnvironment = "SDO_AGENT_REASONING_SUMMARY"

// ReasoningSummaries are the summary modes a worker accepts.
var ReasoningSummaries = settings.ReasoningSummaries

// AgentEfforts are the efforts a worker accepts; sdo/contracts/agent_env.json
// names the same list for the Python workers; a test keeps the two equal.
var AgentEfforts = settings.AgentEfforts

// LifecycleTimeoutEnvironment names the variable that bounds one lifecycle
// (detection designer) session, in whole seconds; workers use the shared default
// (sdo/contracts/agent_env.json) when it is absent.
const LifecycleTimeoutEnvironment = "SDO_LIFECYCLE_TIMEOUT_SECONDS"

// DefaultLifecycleTimeout is the workers' lifecycle session limit when none is configured.
const DefaultLifecycleTimeout = settings.DefaultLifecycleTimeout

// AgentEnvironment is the provider, model, effort and lifecycle session limit a
// controller was configured with, as the environment every agent worker Job
// reads. An unconfigured provider, effort or limit (zero) adds nothing; an
// unknown provider or effort or a negative limit is an error, so no worker runs
// at a setting other than the configured one.
func AgentEnvironment(provider, model, effort, reasoningSummary string, lifecycleTimeout time.Duration) (map[string]string, error) {
	if effort != "" && !slices.Contains(AgentEfforts, effort) {
		return nil, fmt.Errorf("unsupported agent effort %q: must be one of %v", effort, AgentEfforts)
	}
	if reasoningSummary != "" && !slices.Contains(ReasoningSummaries, reasoningSummary) {
		return nil, fmt.Errorf("unsupported reasoning summary %q: must be one of %v", reasoningSummary, ReasoningSummaries)
	}
	if lifecycleTimeout < 0 {
		return nil, fmt.Errorf("invalid lifecycle timeout %s: must not be negative", lifecycleTimeout)
	}
	environment := ProviderCapacityEnvironment()
	if effort != "" {
		environment[EffortEnvironment] = effort
	}
	if reasoningSummary != "" {
		environment[ReasoningSummaryEnvironment] = reasoningSummary
	}
	if lifecycleTimeout > 0 {
		seconds := lifecycleTimeout / time.Second
		if lifecycleTimeout%time.Second > 0 {
			seconds++
		}
		environment[LifecycleTimeoutEnvironment] = strconv.FormatInt(int64(seconds), 10)
	}
	if provider == "" {
		return environment, nil
	}
	if provider != "codex" && provider != "claude" {
		return nil, fmt.Errorf("unsupported responder backend %q: must be codex or claude", provider)
	}
	environment[ProviderEnvironment] = provider
	if model != "" {
		for _, name := range ModelEnvironments {
			environment[name] = model
		}
	}
	return environment, nil
}
