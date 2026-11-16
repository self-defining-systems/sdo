package memory

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

func TestWriteGoalCarriesJourneysFromFrontMatterObjective(t *testing.T) {
	root := t.TempDir()
	objective := "---\nschema_version: 1\njourneys:\n  - id: sign-in\n    what: a visitor signs in\n---\nserve every request\n"
	if err := WriteGoal(root, "toy", objective, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, layout.GoalFile))
	if err != nil {
		t.Fatal(err)
	}
	journeys, err := appmap.ParseGoalJourneys(data)
	if err != nil || len(journeys) != 1 || journeys[0].ID != "sign-in" {
		t.Fatalf("journeys = %+v, %v", journeys, err)
	}
	if got, err := AcceptedHealthObjective(root); err != nil || got != "serve every request\n\nUser journeys:\n- sign-in: a visitor signs in" {
		t.Fatalf("objective = %q, %v", got, err)
	}
	if strings.Count(string(data), "owner: human") != 1 {
		t.Fatalf("goal front matter duplicated: %s", data)
	}
}

func TestWriteGoalRejectsMalformedJourneys(t *testing.T) {
	objective := "---\nschema_version: 1\njourneys:\n  - id: sign-in\n---\nserve\n"
	if err := WriteGoal(t.TempDir(), "toy", objective, nil); err == nil {
		t.Fatal("journey without what accepted")
	}
}

const syntheticJourneys = "users sign in\nusers browse items\nusers check out a cart\n"

func TestFreeTextJourneysReachTheGoalVerbatimAndAsGivenJourneys(t *testing.T) {
	objective, err := ComposeObjective("serve every request", syntheticJourneys)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(objective, syntheticJourneys) {
		t.Fatalf("the agent must get the text as given:\n%s", objective)
	}
	root := t.TempDir()
	if err := WriteGoal(root, "toy", objective, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, layout.GoalFile))
	if err != nil {
		t.Fatal(err)
	}
	journeys, err := appmap.ParseGoalJourneys(data)
	if err != nil || len(journeys) != 3 || journeys[2].What != "users check out a cart" {
		t.Fatalf("given journeys = %+v, %v", journeys, err)
	}
	accepted, err := AcceptedHealthObjective(root)
	if err != nil || !strings.Contains(accepted, "users browse items") || !strings.HasPrefix(accepted, "serve every request") {
		t.Fatalf("later agents read the objective with the journeys: %q, %v", accepted, err)
	}
}

func TestStructuredJourneysAreListedForTheAgentsToo(t *testing.T) {
	root := t.TempDir()
	objective := "---\nschema_version: 1\njourneys:\n  - id: sign-in\n    what: a visitor signs in\n---\nserve every request\n"
	if err := WriteGoal(root, "toy", objective, nil); err != nil {
		t.Fatal(err)
	}
	if got, err := AcceptedHealthObjective(root); err != nil || !strings.Contains(got, "sign-in: a visitor signs in") {
		t.Fatalf("objective = %q, %v", got, err)
	}
}

func TestComposeObjectiveRefusesTwoSourcesOfJourneys(t *testing.T) {
	structured := "---\nschema_version: 1\njourneys:\n  - {id: a, what: x}\n---\nbody\n"
	if _, err := ComposeObjective(structured, syntheticJourneys); err == nil {
		t.Fatal("journeys in the goal and in the input must conflict")
	}
	if got, err := ComposeObjective("body", "  \n"); err != nil || got != "body" {
		t.Fatalf("blank journeys change nothing: %q, %v", got, err)
	}
}

func TestRequireJourneysNamesHowToProvideThemAndHowToOptOut(t *testing.T) {
	for name, objective := range map[string]string{"empty": "", "plain": "serve", "front matter without": "---\nschema_version: 1\n---\nserve\n"} {
		err := RequireJourneys(objective)
		var missing *JourneysRequiredError
		if !errors.As(err, &missing) {
			t.Fatalf("%s: err = %v, want *JourneysRequiredError", name, err)
		}
		for _, hint := range []string{"--journeys", "--journeys-file", "journeys:", "onboarding.journeys_required", "SDO_ONBOARDING_JOURNEYS_REQUIRED"} {
			if !strings.Contains(err.Error(), hint) {
				t.Errorf("%s: message lacks %q: %v", name, hint, err)
			}
		}
	}
	composed, _ := ComposeObjective("serve", syntheticJourneys)
	if err := RequireJourneys(composed); err != nil {
		t.Fatal(err)
	}
}
