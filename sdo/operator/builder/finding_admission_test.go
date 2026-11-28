package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The incident's captured states reach the generated finding-admission test,
// recordings excluded; no capture, no test.
func TestFindingAdmissionStagesEveryCapturedState(t *testing.T) {
	capture, workspace := t.TempDir(), t.TempDir()
	for _, name := range []string{"open@detect.json", "open@detect-recordings.json", "fixed.json"} {
		if err := os.WriteFile(filepath.Join(capture, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := generateFindingAdmissionTest(capture, workspace); err != nil {
		t.Fatal(err)
	}
	staged, _ := filepath.Glob(filepath.Join(workspace, "generated", "testdata", findingAdmissionFixtures, "*", "*.json"))
	var names []string
	for _, path := range staged {
		names = append(names, filepath.Base(path))
	}
	if got := strings.Join(names, ","); got != "open@detect.json,fixed.json" {
		t.Fatalf("staged %s", got)
	}
	source, err := os.ReadFile(filepath.Join(workspace, "generated", "finding_admission_test.go"))
	if err != nil || !strings.Contains(string(source), "core.FindingAdmissionViolations") || !strings.Contains(string(source), FindingAdmissionRule+"[") {
		t.Fatalf("generated test: %v\n%s", err, source)
	}
	empty := t.TempDir()
	if err := generateFindingAdmissionTest("", empty); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(empty, "generated", "finding_admission_test.go")); !os.IsNotExist(err) {
		t.Fatalf("no capture still wrote a test: %v", err)
	}
}
