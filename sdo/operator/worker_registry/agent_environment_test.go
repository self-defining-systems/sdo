package worker_registry

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/contracts/settings"
)

// The Python workers read the same names from sdo/contracts/agent_env.json; a
// drift would make a configured value never reach them.
func TestAgentEnvironmentNamesMatchTheSharedManifest(t *testing.T) {
	data, err := os.ReadFile("../../contracts/agent_env.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Provider         string            `json:"provider"`
		Effort           string            `json:"effort"`
		Efforts          []string          `json:"efforts"`
		Summary          string            `json:"reasoning_summary"`
		Summaries        []string          `json:"reasoning_summaries"`
		Models           map[string]string `json:"models"`
		LifecycleTimeout string            `json:"lifecycle_timeout"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	models := make([]string, 0, len(manifest.Models))
	for _, name := range manifest.Models {
		models = append(models, name)
	}
	slices.Sort(models)
	goModels := slices.Sorted(slices.Values(ModelEnvironments))
	if manifest.Provider != ProviderEnvironment || manifest.Effort != EffortEnvironment || manifest.Summary != ReasoningSummaryEnvironment || !slices.Equal(manifest.Summaries, ReasoningSummaries) || manifest.LifecycleTimeout != LifecycleTimeoutEnvironment ||
		!slices.Equal(manifest.Efforts, AgentEfforts) || !slices.Equal(models, goModels) {
		t.Fatalf("agent_env.json %+v differs from the Go worker environment names", manifest)
	}
}

// Every worker Job carries the configured provider, model, effort and limits, defaults included, so no worker runs
// at a default of its own.
func TestEveryWorkerCarriesTheConfiguredAgentSettings(t *testing.T) {
	agent := settings.Default().Agent
	agent.Model = "m"
	environment := AgentEnvironment(agent)
	want := map[string]string{
		ProviderEnvironment: "codex", EffortEnvironment: "medium", ReasoningSummaryEnvironment: "auto",
		LifecycleTimeoutEnvironment: "3600", MemoryCheckTimeoutEnvironment: "300",
	}
	for _, name := range ModelEnvironments {
		want[name] = "m"
	}
	for name, value := range want {
		if environment[name] != value {
			t.Errorf("%s = %q, want %q", name, environment[name], value)
		}
	}
	agent.LifecycleTimeout = settings.Duration(1500 * time.Millisecond)
	if got := AgentEnvironment(agent)[LifecycleTimeoutEnvironment]; got != "2" {
		t.Fatalf("a sub-second remainder rounds up: %q", got)
	}
}
