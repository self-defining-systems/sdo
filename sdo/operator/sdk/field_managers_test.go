package sdk

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// SDO's field-manager names live only in this package. A literal elsewhere
// could write under a name SecretDataChangedAt does not treat as SDO's own,
// so SDO's writes would read as foreign data changes again.
func TestSDOFieldManagerNamesAreDeclaredOnlyInTheSDK(t *testing.T) {
	literals := []string{`"` + DeployerFieldManager + `"`, `"` + AcceptanceFieldManager + `"`, `"` + ReplicasHandoffFieldManager + `"`, "field-manager=sdo-", `FieldManager: "sdo-`}
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || filepath.Clean(path) == filepath.Join("..", "sdk", "secret.go") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, literal := range literals {
			if strings.Contains(string(source), literal) {
				t.Errorf("%s names field manager %s; use the sdk constant", path, literal)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, manager := range SDOFieldManagers() {
		if !IsSDOFieldManager(manager) || IsSDOFieldManager(BeforeFirstApplyFieldManager) {
			t.Fatalf("SDO manager set is inconsistent at %s", manager)
		}
	}
}
