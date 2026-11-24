package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	programvalidation "sdo.dev/operator/memory/validate/program"
)

// The incident's captured open state reaches the generated tests before Go writes a corpus case, so a program that
// is new in this incident has its plan built and validated on real state.
func TestIncidentCaptureStagesAPlanTestForEveryMitigator(t *testing.T) {
	capture, workspace := t.TempDir(), t.TempDir()
	for name, data := range map[string]string{"open@detect.json": "{}", "open@detect-recordings.json": "[]", "fixed.json": "{}"} {
		if err := os.WriteFile(filepath.Join(capture, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(workspace, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	programs := []programvalidation.Spec{{ID: "fix", Diagnoser: "Program", Mitigator: "Program"}, {ID: "diag-only", Diagnoser: "Program"}}

	if err := generateIncidentPlanTest(capture, workspace, programs); err != nil {
		t.Fatal(err)
	}

	staged, err := filepath.Glob(filepath.Join(workspace, "generated", "testdata", "incident-open", "*"))
	if err != nil || len(staged) != 2 {
		t.Fatalf("staged = %v, %v; want the open@* files only", staged, err)
	}
	source, err := os.ReadFile(filepath.Join(workspace, "generated", "incident_plans_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), `check("fix")`) || strings.Contains(string(source), "diag-only") {
		t.Fatalf("generated test = %s", source)
	}
}

func TestNoIncidentCaptureStagesNothing(t *testing.T) {
	workspace := t.TempDir()
	if err := generateIncidentPlanTest("", workspace, nil); err != nil {
		t.Fatal(err)
	}
	if err := generateIncidentPlanTest(t.TempDir(), workspace, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "generated")); !os.IsNotExist(err) {
		t.Fatalf("staged without a capture: %v", err)
	}
}

// A playbook never verified on its own incident shows in the check output as a coverage note.
func TestUnverifiedIncidentPlansAreCoverageGaps(t *testing.T) {
	workspace := &Workspace{Directory: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(workspace.Directory, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	note := "program fix is not verified on its own incident (open@detect.json: Mitigate could not run on it: binding missing)"
	if err := os.WriteFile(filepath.Join(workspace.Directory, "generated", "incident-plan-gaps.json"), []byte(`["`+note+`"]`), 0o600); err != nil {
		t.Fatal(err)
	}

	gaps, err := workspace.CoverageGaps()

	if err != nil || len(gaps) != 1 || gaps[0] != note {
		t.Fatalf("gaps = %v, %v", gaps, err)
	}
}
