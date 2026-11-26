package operatorcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSDO(t *testing.T, repo, name, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(repo, ".sdo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, ".sdo", name), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

const twoJourneyGoal = "---\nschema_version: 1\nowner: human\njourneys:\n  - {id: sign-in, what: signing in}\n  - {id: browse, what: browse items}\n---\nKeep it healthy.\n"

// The journeys checker reads the goal's journeys in order and the working
// tree's map; a missing or broken map leaves every journey unmapped and says
// why, and a goal whose journeys do not parse stops the check instead of
// dropping them from the checklist.
func TestJourneysCheckReadsTheGoalInOrderAndTheWorkingTreeMap(t *testing.T) {
	repo := t.TempDir()
	writeSDO(t, repo, "goal.md", twoJourneyGoal)
	given, m, mapErr, err := journeyGoalAndMap(repo)
	if err != nil || len(given) != 2 || given[0].ID != "sign-in" || given[1].ID != "browse" || len(m.Operations) != 0 || !strings.Contains(mapErr, "no application map") {
		t.Fatalf("given=%+v map=%+v mapErr=%q err=%v", given, m, mapErr, err)
	}
	writeSDO(t, repo, "arch.md", "# arch\n<!-- sdo:application-map:begin -->\n```yaml\nnot: [valid\n```\n<!-- sdo:application-map:end -->\n")
	if _, _, mapErr, err := journeyGoalAndMap(repo); err != nil || !strings.Contains(mapErr, "application map") {
		t.Fatalf("a broken map is reported: %q %v", mapErr, err)
	}
	writeSDO(t, repo, "goal.md", "---\nschema_version: 1\njourneys:\n  - {id: sign-in}\n---\nbody\n")
	if _, _, _, err := journeyGoalAndMap(repo); err == nil {
		t.Fatal("unparseable journeys must stop the check")
	}
	if given, _, _, err := journeyGoalAndMap(t.TempDir()); err != nil || len(given) != 0 {
		t.Fatalf("no goal: no journeys, no error: %+v %v", given, err)
	}
	unreadable := t.TempDir()
	if err := os.MkdirAll(filepath.Join(unreadable, ".sdo", "goal.md"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := goalJourneys(unreadable); err == nil {
		t.Fatal("a goal that cannot be read is an error, not an empty list (the full check reads it the same way)")
	}
}
