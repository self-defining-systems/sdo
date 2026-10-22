package worker_registry

import (
	"encoding/json"
	"os"
	"reflect"
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
		Provider string            `json:"provider"`
		Models   map[string]string `json:"models"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Provider != ProviderEnvironment {
		t.Errorf("provider variable: manifest %q, Go %q", manifest.Provider, ProviderEnvironment)
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
	environment, err := AgentEnvironment("codex", "m")
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
