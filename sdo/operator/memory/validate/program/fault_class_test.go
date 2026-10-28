package program

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// fault_class is derived from the playbook ID by layout.PlaybookFaultClass and
// normalized before validation. No program rule may contradict that
// derivation, including for the health-objective playbook that health
// detectors link to.
func TestProgramAcceptsEveryDerivedFaultClass(t *testing.T) {
	for _, id := range []string{"mongo-auth-bootstrap", layout.HealthObjectivePlaybook} {
		t.Run(id, func(t *testing.T) {
			root := t.TempDir()
			if err := os.CopyFS(root, os.DirFS("../../../../../tests/fixtures/sdo/hotel_run_script_upgrade/.sdo")); err != nil {
				t.Fatal(err)
			}
			if id != "mongo-auth-bootstrap" {
				if err := os.Rename(filepath.Join(root, "playbooks/mongo-auth-bootstrap"), filepath.Join(root, "playbooks", id)); err != nil {
					t.Fatal(err)
				}
			}
			err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				text := strings.ReplaceAll(string(data), "fault_class: mongo-auth-bootstrap", "fault_class: "+layout.PlaybookFaultClass(id))
				text = strings.ReplaceAll(text, `"mongo-auth-bootstrap"`, `"`+id+`"`)
				text = strings.ReplaceAll(text, "- mongo-auth-bootstrap", "- "+id)
				return os.WriteFile(path, []byte(text), 0o644)
			})
			if err != nil {
				t.Fatal(err)
			}
			specs, err := InspectMemory(root, "fixture-diagnostics")
			if err != nil || len(specs) != 1 || specs[0].ID != id || specs[0].Mitigator == "" {
				t.Fatalf("program in playbook %s with fault_class %s rejected: specs=%v err=%v", id, layout.PlaybookFaultClass(id), specs, err)
			}
		})
	}
}
