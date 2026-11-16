package memory

import (
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
	if got, err := AcceptedHealthObjective(root); err != nil || got != "serve every request" {
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
