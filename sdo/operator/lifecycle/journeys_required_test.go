package lifecycle_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

func TestMissingJourneysStopRunBeforeAnyAgentOrCommit(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	cfg := config()
	cfg.JourneysRequired = true
	_, err := h.coordinator.Run(context.Background(), cfg)
	var missing *memory.JourneysRequiredError
	if !errors.As(err, &missing) {
		t.Fatalf("err = %v, want *memory.JourneysRequiredError", err)
	}
	if h.readiness.calls != 0 || h.workers.deployerAttempts() != 0 || h.workers.judgeRuns != 0 || h.installer.installs != 0 {
		t.Fatalf("readiness=%d deployer=%d judge=%d installs=%d: nothing may run", h.readiness.calls, h.workers.deployerAttempts(), h.workers.judgeRuns, h.installer.installs)
	}
}

func TestJourneysRequiredProceedsWhenGivenAndWhenTheOptionIsOff(t *testing.T) {
	objective, err := memory.ComposeObjective("stay up", "users sign in\nusers browse items")
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*lifecycle.Config){
		"given":      func(c *lifecycle.Config) { c.JourneysRequired, c.HealthObjective = true, objective },
		"option off": func(c *lifecycle.Config) { c.JourneysRequired = false },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
			cfg := config()
			cfg.StopAfterArchitecture = true
			mutate(&cfg)
			result, err := h.coordinator.Run(context.Background(), cfg)
			if err != nil || result.GoalCommit == "" || result.ArchitectureCommit == "" {
				t.Fatalf("result %+v, %v", result, err)
			}
		})
	}
}

func TestTheDesignerGetsTheJourneysAsGivenWithoutFrontMatter(t *testing.T) {
	objective, err := memory.ComposeObjective("stay up", "users sign in\nusers browse items")
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	cfg := config()
	cfg.JourneysRequired, cfg.HealthObjective = true, objective
	if _, err := h.coordinator.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	got := h.workers.judgeRequests[0].GetHealthObjective()
	if !strings.HasPrefix(got, "stay up") || !strings.Contains(got, "User journeys:\nusers sign in\nusers browse items") || strings.Contains(got, "schema_version") {
		t.Fatalf("judge objective = %q", got)
	}
}
