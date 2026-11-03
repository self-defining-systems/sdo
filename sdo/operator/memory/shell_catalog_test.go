package memory

import (
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
)

func TestShellCatalogProjectsNotesAndIgnoresControllerOwnedRecords(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		".sdo/goal.md": "available", ".sdo/arch.md": "application",
		".sdo/diagnostics/detectors/health/ready/check.sh":   "exit 0",
		".sdo/playbooks/notes/custom.md":                     strings.Repeat("unstructured notes\n", 1000),
		".sdo/outcomes/" + strings.Repeat("a", 64) + ".json": strings.Repeat("\x00", 300000),
		".sdo/diagnostics/dependencies/go.sum":               strings.Repeat("x", 300000),
	}
	for path, body := range files {
		absolute := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(absolute), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(body), 0755); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := CandidateShellSnapshot(root, strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Playbooks) != 1 || !strings.Contains(snapshot.Playbooks[0].Markdown, files[".sdo/playbooks/notes/custom.md"]) {
		t.Fatal("free notes lost")
	}
}

func TestSnapshotsAllowNoDetectors(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"schema-version": "3\n", "arch.md": "---\nschema_version: 1\n---\nApplication\n", "goal.md": "---\nschema_version: 1\nowner: human\n---\nAvailable\n"} {
		if err := os.WriteFile(filepath.Join(root, ".sdo", name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for _, snapshot := range []func(string, string) (*contracts.OperationalMemorySnapshot, error){CandidateSnapshot, CandidateShellSnapshot} {
		got, err := snapshot(root, strings.Repeat("a", 40))
		if err != nil || len(got.GetDetectors()) != 0 {
			t.Fatalf("empty detector class: %v %v", got, err)
		}
	}
}
