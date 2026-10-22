package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestRepeatCorpusCopiesEvidenceWithoutChangingProgram(t *testing.T) {
	for _, phase := range []string{"detect", "pre-apply"} {
		t.Run(phase, func(t *testing.T) {
			root, captures := t.TempDir(), t.TempDir()
			before := []byte(`{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"target"}}]}`)
			fixed := []byte(`{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"target"}},{"metadata":{"namespace":"app","name":"peer"}}]}`)
			for name, data := range map[string][]byte{"open@" + phase + ".json": before, "fixed.json": fixed, "open@" + phase + "-recordings.json": []byte("[]\n")} {
				if err := os.WriteFile(filepath.Join(captures, name), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
			prior := filepath.Join(root, ".sdo/corpus/prior")
			if err := os.MkdirAll(prior, 0755); err != nil {
				t.Fatal(err)
			}
			peer := program.Bindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}}
			labels, _ := json.Marshal(map[string]any{"root_cause_program_ids": []string{"routing"}, "peer_bindings": peer})
			if err := os.WriteFile(filepath.Join(prior, "labels.json"), labels, 0644); err != nil {
				t.Fatal(err)
			}
			bindings := map[string][]program.Bindings{"routing": {{"service": sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "target"}}}}
			paths, err := RecordRepeatCorpus(root, captures, "incident", bindings, map[string]string{CapturedInstanceKey("routing", bindings["routing"][0]): phase})
			if err != nil || len(paths) != 5 {
				t.Fatalf("repeat corpus: %v %v", paths, err)
			}
			if err := ValidateCapturedCorpusDirectory(captures, root, "incident", paths); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(root, ".sdo/playbooks")); !os.IsNotExist(err) {
				t.Fatal("repeat added narrative or program changes")
			}

		})
	}
}
