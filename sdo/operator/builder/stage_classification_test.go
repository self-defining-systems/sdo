package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

const advisoryStage = "-trimpath -timeout=30s ./"

// Which toolchain failures reject a workspace is decided by the stage plan:
// acceptance stages cover detectors, traffic generators and generated safety
// tests, build and vet cover every package, and playbook tests only run in the
// informational pass. The synthetic compiler fails a stage exactly when the
// stage's package patterns select a planted file, so a stage that stopped
// covering a directory (or an advisory stage that started rejecting) flips a case.
func TestStageClassificationDecidesAcceptance(t *testing.T) {
	failing := "package p\nimport \"testing\"\nfunc TestSynthetic(t *testing.T) { t.Fatal(\"synthetic failure\") }\n"
	for _, test := range []struct {
		name, verb, dir, file string
		reject                bool
	}{
		{"advisory playbook test", "test", "playbooks/synthetic", "publication_test.go", false},
		{"generated safety test", "test", "generated", "publication_test.go", true},
		{"detector test", "test", "detectors/incidents/missing-configmap", "publication_test.go", true},
		{"traffic generator test", "test", "traffic/generators", "publication_test.go", true},
		{"authored compile", "build", "playbooks/synthetic", "publication.go", true},
		{"authored vet", "vet", "playbooks/synthetic", "publication.go", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			testutil.FakeGo(t, testutil.GoRule{Verb: test.verb, Covers: test.dir, Marker: filepath.Join(test.dir, test.file), Output: "synthetic stage failure"})
			w := acceptanceFixture(t)
			if err := os.MkdirAll(filepath.Join(w.Directory, test.dir), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(w.Directory, test.dir, test.file), []byte(failing), 0o644); err != nil {
				t.Fatal(err)
			}
			acceptanceErr := w.CheckAcceptance(context.Background())
			for _, stage := range w.StageTimings {
				if strings.Contains(stage.Stage, advisoryStage) {
					t.Fatalf("acceptance ran the informational pass: %s", stage.Stage)
				}
			}
			fullErr := w.Check(context.Background())
			if (acceptanceErr != nil) != test.reject || (fullErr != nil) != test.reject {
				t.Fatalf("acceptance=%v check=%v want reject=%v", acceptanceErr, fullErr, test.reject)
			}
			if !test.reject && len(w.advisoryTestErrors) == 0 {
				t.Fatal("explicit self-check lost the advisory failure")
			}
		})
	}
}
