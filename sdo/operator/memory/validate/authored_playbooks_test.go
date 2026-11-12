package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func writePlaybookReadme(t *testing.T, root, id, content string) string {
	t.Helper()
	path := layout.PlaybookDir(id) + "/" + layout.PlaybookFileName
	full := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func readPlaybookReadme(t *testing.T, root, path string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// Provenance is Go's to write: the agent supplies the prose, SDO the owner,
// the schema stamp and the incident the playbook was learned in.
func TestNormalizeAuthoredPlaybooksStampsProvenance(t *testing.T) {
	root := t.TempDir()
	path := writePlaybookReadme(t, root, "restart-api", "---\nfault_class: wrong\n---\nRestart the stale process.\n")
	if _, err := NormalizeAuthoredPlaybooks(root, []string{path}, "incident-7"); err != nil {
		t.Fatal(err)
	}
	got := readPlaybookReadme(t, root, path)
	for _, want := range []string{"schema_version: 1\n", "owner: responder\n", "originating_incident: incident-7\n", "fault_class: restart-api\n", "Restart the stale process."} {
		if !strings.Contains(got, want) {
			t.Fatalf("README = %q, want %q", got, want)
		}
	}
	if _, err := layout.ParseDocument([]byte(got)); err != nil {
		t.Fatalf("normalized README does not parse: %v", err)
	}
}

func TestNormalizeAuthoredPlaybooksKeepsWhatIsSet(t *testing.T) {
	root := t.TempDir()
	original := "---\nschema_version: 1\nowner: responder\nfault_class: keep\noriginating_incident: first\n---\nBody.\n"
	path := writePlaybookReadme(t, root, "keep", original)
	if _, err := NormalizeAuthoredPlaybooks(root, []string{path}, "later-incident"); err != nil {
		t.Fatal(err)
	}
	if got := readPlaybookReadme(t, root, path); got != original {
		t.Fatalf("README rewritten to %q", got)
	}
}

func TestNormalizeAuthoredPlaybooksWithoutAnIncidentLeavesProvenanceAlone(t *testing.T) {
	root := t.TempDir()
	path := writePlaybookReadme(t, root, "no-incident", "---\nschema_version: 1\nfault_class: no-incident\n---\nBody.\n")
	if _, err := NormalizeAuthoredPlaybooks(root, []string{path}, ""); err != nil {
		t.Fatal(err)
	}
	if got := readPlaybookReadme(t, root, path); strings.Contains(got, "originating_incident") {
		t.Fatalf("README = %q, want no invented incident", got)
	}
}
