package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Run a real generated workspace through both entry points. An authored test
// failure remains advisory; a generated safety failure rejects both paths.
func TestPublicationCheckPreservesAcceptanceWithoutInformationalPass(t *testing.T) {
	for _, safetyFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "advisory failure", true: "safety failure"}[safetyFailure], func(t *testing.T) {
			w := acceptanceFixture(t)
			pkg := "playbooks/synthetic"
			if safetyFailure {
				pkg = "generated"
			}
			dir := filepath.Join(w.Directory, pkg)
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			name := "synthetic"
			if safetyFailure {
				name = "generated"
			}
			source := "package " + name + "\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n"
			if err := os.WriteFile(filepath.Join(dir, "publication_test.go"), []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			publicationErr := w.CheckAcceptance(context.Background())
			for _, stage := range w.StageTimings {
				if strings.Contains(stage.Stage, "informational") {
					t.Fatal("publication ran advisory tests")
				}
			}
			legacyErr := w.Check(context.Background())
			if (publicationErr != nil) != safetyFailure || (legacyErr != nil) != safetyFailure {
				t.Fatalf("acceptance changed: publication=%v legacy=%v safetyFailure=%v", publicationErr, legacyErr, safetyFailure)
			}
		})
	}
}
