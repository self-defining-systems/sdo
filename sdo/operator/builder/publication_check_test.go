package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Real generated workspaces exercise every required gate, plus an authored
// failure that is advisory. Both public checking paths must make the same
// acceptance decision; only explicit self-checks execute informational tests.
func TestPublicationCheckPreservesAcceptanceWithoutInformationalPass(t *testing.T) {
	for _, test := range []struct {
		name, path, source string
		reject             bool
	}{
		{"advisory", "playbooks/synthetic/publication_test.go", "package synthetic\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n", false},
		{"generated safety", "generated/publication_test.go", "package generated\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n", true},
		{"detector", "detectors/incidents/missing-configmap/publication_test.go", "package detector\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n", true},
		{"traffic", "traffic/generators/publication_test.go", "package generators\nimport \"testing\"\nfunc TestSyntheticPublicationContract(t *testing.T) { t.Fatal(\"synthetic failure\") }\n", true},
		{"authored compile", "playbooks/synthetic/publication.go", "package synthetic\nvar _ = syntheticUndefined\n", true},
		{"authored vet", "playbooks/synthetic/publication.go", "package synthetic\nimport \"fmt\"\nfunc SyntheticVetFailure() { fmt.Printf(\"%d\", \"synthetic\") }\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := acceptanceFixture(t)
			path := filepath.Join(w.Directory, test.path)
			// The fixture detector has a package name owned by its source.
			source := test.source
			if test.name == "detector" {
				data, err := os.ReadFile(filepath.Join(filepath.Dir(path), "detector.go"))
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range strings.Split(string(data), "\n") {
					if strings.HasPrefix(line, "package ") {
						source = strings.Replace(source, "package detector", line, 1)
						break
					}
				}
			}
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
