package validate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const namedManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: cause
constructor: New
class: learned
owner: responder
watches:
  - apiVersion: apps/v1
    kind: Deployment
interval: 30s
persistence:
  firing: 1
  clearing: 2
batching:
  severity: critical
  debounce: "0"
possiblePlaybooks: PLAYBOOKS
originatingIncident: synthetic
originatingCommit: synthetic
`

func namedPlaybookRoot(t *testing.T, playbooks string, existing ...string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, ".sdo/diagnostics/detectors/learned/cause")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := strings.Replace(namedManifest, "PLAYBOOKS", playbooks, 1)
	if err := os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, id := range existing {
		book := filepath.Join(root, ".sdo/playbooks", id)
		if err := os.MkdirAll(book, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(book, "README.md"), []byte("# x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

var causeDetectorPaths = []string{".sdo/diagnostics/detectors/learned/cause/manifest.yaml"}

func TestNamedPlaybookDiagnostics(t *testing.T) {
	for _, row := range []struct {
		name, playbooks string
		existing        []string
		want            string
	}{
		{"names an existing playbook", "[fix]", []string{"fix"}, ""},
		{"names no playbook", "[]", []string{"fix"}, RuleDetectorNamesNoPlaybook},
		{"names no playbook in an empty memory", "[]", nil, RuleDetectorNamesNoPlaybook},
		{"names a playbook that does not exist in an empty memory", "[fix]", nil, RuleUnknownPlaybook},
		// With playbooks present the semantic check owns the missing-playbook
		// rule; this one stays quiet so it is reported once.
		{"names a missing playbook beside others", "[gone]", []string{"fix"}, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			got := namedPlaybookDiagnostics(namedPlaybookRoot(t, row.playbooks, row.existing...), causeDetectorPaths)
			if row.want == "" && len(got) != 0 || row.want != "" && (len(got) != 1 || got[0].Rule != row.want) {
				t.Fatalf("diagnostics %v, want %q", got, row.want)
			}
		})
	}
}
