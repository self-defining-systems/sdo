package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A playbook new in this change has its Diagnose run on the incident's capture; with no new playbook or no
// capture nothing is staged, so an ordinary check pays nothing.
func TestOwnIncidentTestIsStagedOnlyForNewPlaybooksWithACapture(t *testing.T) {
	capture := t.TempDir()
	for name, data := range map[string]string{"open@detect.json": "{}", "open@detect-recordings.json": "[]", "fixed.json": "{}"} {
		if err := os.WriteFile(filepath.Join(capture, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for name, tc := range map[string]struct {
		capture string
		ids     []string
		staged  bool
	}{
		"capture and a new playbook": {capture, []string{"fresh"}, true},
		"no new playbook":            {capture, nil, false},
		"no capture":                 {"", []string{"fresh"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			workspace := t.TempDir()
			if err := os.MkdirAll(filepath.Join(workspace, "generated"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := generateOwnIncidentTest(tc.capture, workspace, tc.ids); err != nil {
				t.Fatal(err)
			}
			source, err := os.ReadFile(filepath.Join(workspace, "generated", "own_incident_test.go"))
			if !tc.staged {
				if !os.IsNotExist(err) {
					t.Fatalf("staged without need: %v", err)
				}
				return
			}
			if err != nil || !strings.Contains(string(source), `check("fresh")`) || !strings.Contains(string(source), "playbook.own-incident-replay[%s]") {
				t.Fatalf("generated test = %s, %v", source, err)
			}
			staged, _ := filepath.Glob(filepath.Join(workspace, "generated", "testdata", "own-incident", "open@*.json"))
			if len(staged) != 2 {
				t.Fatalf("staged captures = %v, want the open@* files only", staged)
			}
		})
	}
}

func TestOwnIncidentResultsAreReadBack(t *testing.T) {
	workspace := &Workspace{Directory: t.TempDir()}
	if got, err := workspace.OwnIncidentResults(); err != nil || got != nil {
		t.Fatalf("no file: %v, %v", got, err)
	}
	if err := os.MkdirAll(filepath.Join(workspace.Directory, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Directory, "generated", "own-incident-replay.json"), []byte(`[{"program_id":"fresh","outcome":"unverified","reason":"x"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := workspace.OwnIncidentResults()
	if err != nil || len(got) != 1 || got[0].ProgramID != "fresh" || got[0].Outcome != "unverified" {
		t.Fatalf("results = %+v, %v", got, err)
	}
}
