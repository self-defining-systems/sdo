package operatorcli

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/operatorcli/internal/processio"
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

func TestQuickJourneysRequiresOneExactScenario(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{
		{"--quick"},
		{"--quick", "--only", "read,write"},
		{"--quick", "--only", "read", "--plan"},
		{"--quick", "--only", "read", "--alter"},
		{"--quick", "--only", "read", "--soak", "2s"},
	} {
		_, err := onboardingJourneys(context.Background(), append([]string{"--repo", repo, "--namespace", "example"}, args...), io.Discard, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "--quick requires") {
			t.Errorf("%v: got %v", args, err)
		}
	}
}

func TestQuickJourneysStopsAtTheSharedMemoryCheck(t *testing.T) {
	repo := parityRepo(t)
	t.Setenv("SDO_ACTOR", "invalid-actor")
	t.Setenv("SDO_PHASE", "initial")
	t.Setenv("SDO_BASE_COMMIT", "HEAD")
	var check, quick, checkErr, quickErr bytes.Buffer
	code := RunMemory(context.Background(), []string{"check", "--root", repo, "--replay", "off"}, processio.Environment, &check, &checkErr)
	if code == exitClean {
		t.Fatal("expected the shared check to reject an invalid actor")
	}
	got := RunOnboarding(context.Background(), []string{"journeys", "--repo", repo, "--namespace", "example", "--quick", "--only", "read"}, &quick, &quickErr)
	if got != code || quickErr.String() != checkErr.String() || quick.String() != check.String() {
		t.Fatalf("got (%d, %q, %q), want (%d, %q, %q)", got, &quickErr, &quick, code, &checkErr, &check)
	}
}

// Inside an onboarding session the installed controller soaks the export after
// the session ends, so an in-session --soak is refused at once with that reason
// before anything is built or sent. People outside a session keep --soak.
func TestJourneysSoakInAnOnboardingSessionSaysTheControllerSoaks(t *testing.T) {
	t.Setenv("SDO_ACTOR", "health_judge")
	t.Setenv("SDO_PHASE", "initial")
	repo := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := RunOnboarding(context.Background(), []string{"journeys", "--repo", repo, "--namespace", "example", "--alter", "--soak", "5m"}, &stdout, &stderr)
	if code != exitInfra || stdout.Len() != 0 || !strings.Contains(stderr.String(), "after this session") || strings.Count(stderr.String(), "\n") > 1 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, &stdout, &stderr)
	}
	t.Setenv("SDO_ACTOR", "")
	stderr.Reset()
	RunOnboarding(context.Background(), []string{"journeys", "--repo", repo, "--namespace", "example", "--soak", "5m"}, io.Discard, &stderr)
	if strings.Contains(stderr.String(), "after this session") {
		t.Fatalf("outside a session --soak is not refused: %q", &stderr)
	}
}
