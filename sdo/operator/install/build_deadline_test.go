package install

import (
	"slices"
	"testing"
	"time"
)

func TestBuildDeadlinesDefaultToRealSourceBuilds(t *testing.T) {
	if got := buildTimeout(Config{}); got < 30*time.Minute {
		t.Fatalf("default per-build deadline %s is shorter than the heaviest known source build needs", got)
	}
	if buildBudget(Config{}) < buildTimeout(Config{}) {
		t.Fatal("the shared budget must cover at least one build")
	}
	if got := buildTimeout(Config{BuildTimeoutSeconds: 90}); got != 90*time.Second {
		t.Fatalf("configured deadline ignored: %s", got)
	}
}

func TestBuildDeadlinesValidateTogether(t *testing.T) {
	cfg := Config{BuildTimeoutSeconds: 600, BuildBudgetSeconds: 300}
	if err := cfg.validate(); err == nil {
		t.Fatal("a per-build deadline above the budget must be rejected")
	}
}

func TestOperateJobCarriesBuildDeadlines(t *testing.T) {
	cfg := Config{BuildTimeoutSeconds: 1200, BuildBudgetSeconds: 2400}
	args := []string{"--build-timeout", "1200s", "--build-budget", "2400s", "--rollout-timeout", "600s"}
	got := operateBuildArgs(cfg)
	if !slices.Equal(got, args) {
		t.Fatalf("operate args = %v, want %v", got, args)
	}
}

func TestRolloutTimeoutIsConfigurableAndSizedForInitContainers(t *testing.T) {
	if got := rolloutTimeout(Config{}); got < 8*time.Minute {
		t.Fatalf("default rollout wait %s cannot cover a 3 minute init container with margin", got)
	}
	if got := rolloutTimeout(Config{RolloutTimeoutSeconds: 90}); got != 90*time.Second {
		t.Fatalf("configured rollout wait ignored: %s", got)
	}
	if !slices.Contains(operateBuildArgs(Config{RolloutTimeoutSeconds: 900}), "900s") {
		t.Fatal("operate Job must carry --rollout-timeout")
	}
	if err := (Config{RolloutTimeoutSeconds: -1}).validate(); err == nil {
		t.Fatal("negative rollout timeout must be rejected")
	}
}
