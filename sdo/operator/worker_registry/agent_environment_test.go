package worker_registry

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"testing"
)

// The Python workers read the environment names from sdo/contracts/agent_env.json;
// the Go side writes them into the Job. The two lists must be the same set.
func TestAgentEnvironmentNamesMatchPythonManifest(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/agent_env.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Provider      string            `json:"provider"`
		Effort        string            `json:"effort"`
		DefaultEffort string            `json:"default_effort"`
		Efforts       []string          `json:"efforts"`
		Models        map[string]string `json:"models"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Provider != ProviderEnvironment {
		t.Errorf("provider variable: manifest %q, Go %q", manifest.Provider, ProviderEnvironment)
	}
	if manifest.Effort != EffortEnvironment {
		t.Errorf("effort variable: manifest %q, Go %q", manifest.Effort, EffortEnvironment)
	}
	if !reflect.DeepEqual(manifest.Efforts, AgentEfforts) || !slices.Contains(AgentEfforts, manifest.DefaultEffort) {
		t.Errorf("efforts differ: manifest %v (default %q), Go %v", manifest.Efforts, manifest.DefaultEffort, AgentEfforts)
	}
	manifestModels := map[string]bool{}
	for _, name := range manifest.Models {
		manifestModels[name] = true
	}
	goModels := map[string]bool{}
	for _, name := range ModelEnvironments {
		goModels[name] = true
	}
	if !reflect.DeepEqual(manifestModels, goModels) {
		t.Errorf("model variables differ: manifest %v, Go %v", manifestModels, goModels)
	}
}

func TestAgentEnvironmentSetsEveryModelVariable(t *testing.T) {
	environment, err := AgentEnvironment("codex", "m", "")
	if err != nil {
		t.Fatal(err)
	}
	if environment[ProviderEnvironment] != "codex" {
		t.Fatalf("provider missing: %v", environment)
	}
	for _, name := range ModelEnvironments {
		if environment[name] != "m" {
			t.Errorf("%s = %q", name, environment[name])
		}
	}
	if len(environment) != len(ModelEnvironments)+1 {
		t.Errorf("unexpected extra variables: %v", environment)
	}
}

func TestAgentEnvironmentCarriesTheConfiguredEffortToEveryWorker(t *testing.T) {
	environment, err := AgentEnvironment("claude", "claude-opus-5-5", "medium")
	if err != nil {
		t.Fatal(err)
	}
	if environment[EffortEnvironment] != "medium" || environment[ProviderEnvironment] != "claude" {
		t.Fatalf("effort or provider missing: %v", environment)
	}
	if environment, err = AgentEnvironment("", "", "high"); err != nil || environment[EffortEnvironment] != "high" {
		t.Fatalf("effort without provider: %v, %v", environment, err)
	}
}

func TestAgentEnvironmentRefusesAnUnknownEffort(t *testing.T) {
	for _, effort := range []string{"extreme", "Medium", " medium"} {
		if _, err := AgentEnvironment("claude", "m", effort); err == nil {
			t.Errorf("effort %q accepted", effort)
		}
	}
}
