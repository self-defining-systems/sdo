package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Real compiler conformance for the acceptance decision: a failing authored
// test is advisory and a real vet failure rejects. Which stage covers which
// directory is table-tested over a synthetic compiler in
// TestStageClassificationDecidesAcceptance. Both public checking paths must make the same
// acceptance decision; only explicit self-checks execute informational tests.
func TestPublicationCheckPreservesAcceptanceWithoutInformationalPass(t *testing.T) {
	for _, test := range []struct {
		name, path, source string
		reject             bool
	}{
		{"advisory", "playbooks/synthetic/publication_test.go", "package synthetic\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n", false},
		{"authored vet", "playbooks/synthetic/publication.go", "package synthetic\nimport \"fmt\"\nfunc SyntheticVetFailure() { fmt.Printf(\"%d\", \"synthetic\") }\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := acceptanceFixture(t)
			path := filepath.Join(w.Directory, test.path)
			source := test.source
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			publicationErr := w.CheckAcceptance(context.Background())
			for _, stage := range w.StageTimings {
				if strings.Contains(stage.Stage, "informational") {
					t.Fatal("publication ran advisory tests")
				}
			}
			legacyErr := w.Check(context.Background())
			if (publicationErr != nil) != test.reject || (legacyErr != nil) != test.reject {
				t.Fatalf("acceptance changed: publication=%v legacy=%v reject=%v", publicationErr, legacyErr, test.reject)
			}
			if !test.reject && len(w.advisoryTestErrors) == 0 {
				t.Fatal("explicit self-check lost advisory failure")
			}
		})
	}
}
