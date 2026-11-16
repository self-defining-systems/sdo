package operatorcli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/appmap"
)

var journeysRequired = []string{"SDO_ONBOARDING_JOURNEYS_REQUIRED=true"}

func TestHostWithoutJourneysStopsWithAnActionableError(t *testing.T) {
	_, err := ParseHostLaunchEnv(hostArgs(), journeysRequired)
	var missing *memory.JourneysRequiredError
	if !errors.As(err, &missing) {
		t.Fatalf("err = %v, want *memory.JourneysRequiredError", err)
	}
	for _, hint := range []string{"--journeys", "--goal-file", "SDO_ONBOARDING_JOURNEYS_REQUIRED=false"} {
		if !strings.Contains(err.Error(), hint) {
			t.Errorf("error lacks %q: %v", hint, err)
		}
	}
	// The default is on: with no configuration beyond the model, the requirement holds.
	var bare []string
	for _, arg := range hostArgs() {
		if arg != "--config" && arg != testModelConfig {
			bare = append(bare, arg)
		}
	}
	if _, err := ParseHostLaunchEnv(bare, []string{"SDO_AGENT_MODEL=m"}); !errors.As(err, &missing) {
		t.Fatalf("default config: err = %v", err)
	}
}

func TestHostProceedsWhenTheRequirementIsOff(t *testing.T) {
	if _, err := ParseHostLaunchEnv(hostArgs(), []string{"SDO_ONBOARDING_JOURNEYS_REQUIRED=false"}); err != nil {
		t.Fatal(err)
	}
	off := configFile(t, "onboarding:\n  journeys_required: false\n")
	launch, err := ParseHostLaunchEnv(hostArgs("--config", off), journeysRequired[:0])
	if err != nil || launch.Objective != "stay healthy" {
		t.Fatalf("objective = %q, %v", launch.Objective, err)
	}
}

func TestHostJourneysTextReachesTheObjectiveAsGiven(t *testing.T) {
	text := "users sign in\nusers browse projects\nusers push code\n"
	launch, err := ParseHostLaunchEnv(hostArgs("--journeys", text), journeysRequired)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(launch.Objective, text) || !strings.HasPrefix(strings.SplitN(launch.Objective, "---\n", 3)[2], "stay healthy") {
		t.Fatalf("objective = %q", launch.Objective)
	}
	given, err := memory.ObjectiveJourneys(launch.Objective)
	if err != nil || len(given) != 3 || given[1].What != "users browse projects" {
		t.Fatalf("journeys = %+v, %v", given, err)
	}
	path := filepath.Join(t.TempDir(), "journeys.txt")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	fromFile, err := ParseHostLaunchEnv(hostArgs("--journeys-file", path), journeysRequired)
	if err != nil || fromFile.Objective != launch.Objective {
		t.Fatalf("file objective = %q, %v", fromFile.Objective, err)
	}
	if _, err := ParseHostLaunchEnv(hostArgs("--journeys", text, "--journeys-file", path), journeysRequired); err == nil {
		t.Fatal("two journeys inputs must conflict")
	}
	if _, err := ParseHostLaunchEnv(hostArgs("--journeys-file", "relative.txt"), journeysRequired); err == nil {
		t.Fatal("the journeys file path must be absolute")
	}
}

func TestHostGoalFileJourneysFieldSatisfiesTheRequirement(t *testing.T) {
	goal := filepath.Join(t.TempDir(), "goal.md")
	body := "---\nschema_version: 1\njourneys:\n  - {id: sign-in, what: users sign in}\n---\nstay up\n"
	if err := os.WriteFile(goal, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{}
	for i := 0; i < len(hostArgs()); i++ {
		if hostArgs()[i] == "--health-objective" {
			i++
			continue
		}
		args = append(args, hostArgs()[i])
	}
	launch, err := ParseHostLaunchEnv(append(args, "--goal-file", goal), journeysRequired)
	if err != nil {
		t.Fatal(err)
	}
	if given, _ := appmap.ParseGoalJourneys([]byte(launch.Objective)); len(given) != 1 {
		t.Fatalf("objective = %q", launch.Objective)
	}
}

func TestOperateMountedRequirementReachesTheLifecycle(t *testing.T) {
	launch, err := ParseOperateLaunch(operateArgs())
	if err != nil || launch.Lifecycle.JourneysRequired {
		t.Fatalf("test mount has it off: %v, %v", launch.Lifecycle.JourneysRequired, err)
	}
	withMountedSettings(t, func(c *settings.Config) { c.Onboarding.JourneysRequired = true })
	launch, err = ParseOperateLaunch(operateArgs())
	if err != nil || !launch.Lifecycle.JourneysRequired {
		t.Fatalf("mounted on: %v, %v", launch.Lifecycle.JourneysRequired, err)
	}
}
