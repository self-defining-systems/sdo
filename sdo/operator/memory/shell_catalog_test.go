package memory

import (
	"os"
	"path/filepath"
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
