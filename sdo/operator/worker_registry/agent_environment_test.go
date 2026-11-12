package worker_registry

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// The Python workers read the same names from sdo/contracts/agent_env.json; a
// drift would make a configured value silently fall back to a default.
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
		DefaultTimeout   int               `json:"default_lifecycle_timeout_seconds"`
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
		time.Duration(manifest.DefaultTimeout)*time.Second != DefaultLifecycleTimeout ||
		!slices.Equal(manifest.Efforts, AgentEfforts) || !slices.Equal(models, goModels) {
		t.Fatalf("agent_env.json %+v differs from the Go worker environment names", manifest)
	}
}

func TestConfiguredLifecycleTimeoutTravelsToEveryWorker(t *testing.T) {
	environment, err := AgentEnvironment("codex", "", "xhigh", "", 45*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := environment[LifecycleTimeoutEnvironment]; got != "2700" {
		t.Fatalf("lifecycle timeout = %q, want 2700", got)
	}
	unset, err := AgentEnvironment("codex", "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := unset[LifecycleTimeoutEnvironment]; present {
		t.Fatal("an unconfigured lifecycle timeout must leave the worker default")
	}
	rounded, err := AgentEnvironment("", "", "", "", 1500*time.Millisecond)
	if err != nil || rounded[LifecycleTimeoutEnvironment] != "2" {
		t.Fatalf("a sub-second remainder rounds up: %v %v", rounded, err)
	}
	if _, err := AgentEnvironment("", "", "", "", -time.Second); err == nil || !strings.Contains(err.Error(), "lifecycle timeout") {
		t.Fatalf("negative lifecycle timeout: %v", err)
	}
}

func TestReasoningSummaryTravelsToEveryWorker(t *testing.T) {
	environment, err := AgentEnvironment("codex", "", "", "concise", 0)
	if err != nil || environment[ReasoningSummaryEnvironment] != "concise" {
		t.Fatalf("environment = %v, %v", environment, err)
	}
	if unset, err := AgentEnvironment("codex", "", "", "", 0); err != nil || unset[ReasoningSummaryEnvironment] != "" {
		t.Fatalf("an unset summary adds nothing: %v, %v", unset, err)
	}
	if _, err := AgentEnvironment("codex", "", "", "verbose", 0); err == nil {
		t.Fatal("an unknown summary mode is refused")
	}
}
