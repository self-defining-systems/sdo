package memory

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestFixedNoMatchEvidenceUsesExactFixedCaptureAndNoTrustLabel(t *testing.T) {
	for _, phase := range []string{"detect", "pre-apply"} {
		t.Run(phase, func(t *testing.T) {
			root, capture := t.TempDir(), t.TempDir()
			fixed := capturefixture.FromView(t, `{"namespace":"app","services":[]}`)
			if err := os.WriteFile(filepath.Join(capture, "fixed.json"), fixed, 0644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(capture, "open@"+phase+"-recordings.json"), []byte("[]"), 0644); err != nil {
				t.Fatal(err)
			}
			instances := map[string][]program.Bindings{"routing": {{Objects: program.ObjectBindings{"service": {Kind: "Service", Namespace: "app", Name: "api"}}}}}
			origins := map[string]string{CapturedInstanceKey("routing", instances["routing"][0]): phase}
			paths, err := RecordFixedNoMatchCorpus(root, capture, "incident", instances, origins)
			if err != nil || len(paths) != 3 {
				t.Fatalf("fixed negative: %v %v", paths, err)
			}
			assertSnapshotsStoredOnce(t, filepath.Join(root, filepath.Dir(paths[0])), "fixed.json")
			if err := ValidateCapturedCorpusDirectory(capture, root, "incident", paths); err != nil {
				t.Fatal(err)
			}
			corpus, err := sdktest.LoadCorpusCase(filepath.Dir(filepath.Join(root, paths[0])))
			if err != nil || len(corpus.RootCauseProgramIDs) != 0 || len(corpus.NoMatchProgramIDs) != 1 || corpus.Provenance.OpenAt != "fixed" {
				t.Fatalf("negative=%+v err=%v", corpus, err)
			}

		})
	}
}
