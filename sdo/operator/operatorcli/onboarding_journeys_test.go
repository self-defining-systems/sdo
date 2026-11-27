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

// journeysRepo is parityRepo with the one generator its workload names, so the
// probes compile.
func journeysRepo(t *testing.T) string {
	t.Helper()
	repo := parityRepo(t)
	dir := filepath.Join(repo, ".sdo", "diagnostics", "traffic", "generators")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := "package generators\n\nimport \"sdo.dev/operator/sdk/traffic\"\n\nfunc Scenarios() traffic.Catalog {\n\treturn traffic.Catalog{\n\t\t{\n\t\t\tID: \"a\", Description: \"Read items.\",\n\t\t\tTarget: traffic.Target{Service: \"web\", Port: 80},\n\t\t\tDependsOn: []string{\"web\"}, SideEffect: traffic.SideEffectRead,\n\t\t\tSteps: []traffic.Step{{Name: \"get\", Endpoint: traffic.GET(\"/items\", nil)}},\n\t\t\tDetects: []traffic.FaultClass{traffic.FaultWrongBody},\n\t\t},\n\t}\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "generators.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	return repo
}

func operatorSourceRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// A journeys run that is not --quick runs the shared memory check too and
// reports its rejection beside the probe results: the probes still run, each
// blocking error is one red line, notes are counted, and the exit is nonzero.
func TestJourneysReportsAMemoryCheckRejectionBesideTheProbeResults(t *testing.T) {
	repo := journeysRepo(t)
	t.Setenv("SDO_ACTOR", "invalid-actor")
	t.Setenv("SDO_PHASE", "initial")
	t.Setenv("SDO_BASE_COMMIT", "HEAD")
	args := []string{"journeys", "--repo", repo, "--namespace", "example", "--plan", "--operator-root", operatorSourceRoot(t)}
	var out, errOut bytes.Buffer
	if code := RunOnboarding(context.Background(), args, &out, &errOut); code != exitDiagnostics {
		t.Fatalf("exit %d, want %d: %q %q", code, exitDiagnostics, &out, &errOut)
	}
	if !strings.Contains(out.String(), "red memory-check: ") || !strings.Contains(out.String(), "planned ") {
		t.Fatalf("want a red memory-check line and the probe plan:\n%s", &out)
	}
	var asJSON bytes.Buffer
	RunOnboarding(context.Background(), append(args, "--json"), &asJSON, io.Discard)
	if !strings.Contains(asJSON.String(), `"item":"memory-check"`) {
		t.Fatalf("--json carries the memory check: %s", &asJSON)
	}
}

// Outside a session the check's environment is unset: one short line says the
// memory check was skipped and the probe run goes on.
func TestJourneysSkipsTheMemoryCheckWithoutItsEnvironment(t *testing.T) {
	repo := journeysRepo(t)
	for _, key := range []string{"SDO_ACTOR", "SDO_PHASE", "SDO_BASE_COMMIT"} {
		t.Setenv(key, "")
	}
	var out bytes.Buffer
	args := []string{"journeys", "--repo", repo, "--namespace", "example", "--plan", "--operator-root", operatorSourceRoot(t)}
	if code := RunOnboarding(context.Background(), args, &out, io.Discard); code != exitClean && code != exitDiagnostics {
		t.Fatalf("exit %d: %s", code, &out)
	}
	if !strings.Contains(out.String(), "memory check skipped: ") || strings.Contains(out.String(), "red memory-check") {
		t.Fatalf("want one skipped line:\n%s", &out)
	}
}

func TestMemoryFlagErrorsAreOneShortLine(t *testing.T) {
	var out, errOut bytes.Buffer
	code := RunMemory(context.Background(), []string{"check", "--repo", "/work"}, nil, &out, &errOut)
	line := strings.TrimSpace(errOut.String())
	if code != exitInfra || out.Len() != 0 || strings.Contains(line, "\n") || strings.Contains(line, "usage:") ||
		!strings.Contains(line, "--root DIR") || !strings.Contains(line, "--help") {
		t.Fatalf("code %d, stdout %q, stderr %q", code, &out, line)
	}
	errOut.Reset()
	RunMemory(context.Background(), []string{"check", "--nope"}, nil, &out, &errOut)
	line = strings.TrimSpace(errOut.String())
	if strings.Contains(line, "\n") || !strings.Contains(line, "--replay") || !strings.Contains(line, "--help") {
		t.Fatalf("stderr %q", line)
	}
	errOut.Reset()
	RunMemory(context.Background(), []string{"bogus"}, nil, &out, &errOut)
	if strings.Contains(errOut.String(), "usage:") {
		t.Fatalf("an unknown command does not print the whole usage: %q", &errOut)
	}
}
