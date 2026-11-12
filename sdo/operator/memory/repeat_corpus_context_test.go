package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func writeCaptureFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func component(name string) program.Bindings {
	return program.Bindings{Objects: program.ObjectBindings{"component": sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: name}}}
}

// A case Go writes carries the incident's own labels: what opened it and which
// components were suspect, so a later reader never re-derives them from prose.
func TestRepeatCorpusRecordsIncidentContext(t *testing.T) {
	root, captures := t.TempDir(), t.TempDir()
	prior, _ := json.Marshal(map[string]any{"root_cause_program_ids": []string{"fix"}, "bindings": component("api")})
	writeCaptureFile(t, filepath.Join(root, layout.CorpusDir, "earlier", "labels.json"), string(prior))
	for _, name := range []string{"fixed.json", "open@detect.json"} {
		writeCaptureFile(t, filepath.Join(captures, name), "{}")
	}
	writeCaptureFile(t, filepath.Join(captures, "open@detect-recordings.json"), "[]")
	suspects := []sdk.ObjectRef{{Kind: "Deployment", Namespace: "app", Name: "front"}}
	paths, err := RecordCorpus(root, captures, "incident-1", map[string][]program.Bindings{"fix": {component("front")}}, nil,
		CaseContext{OpeningKeys: []string{"traffic/browse"}, Suspects: suspects})
	if err != nil || len(paths) == 0 {
		t.Fatalf("RecordCorpus = %v, %v", paths, err)
	}
	var labelsPath string
	for _, path := range paths {
		if filepath.Base(path) == "labels.json" {
			labelsPath = path
		}
	}
	data, err := os.ReadFile(filepath.Join(root, labelsPath))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		OpeningKeys []string        `json:"opening_keys"`
		Suspects    []sdk.ObjectRef `json:"suspects"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.OpeningKeys) != 1 || got.OpeningKeys[0] != "traffic/browse" || len(got.Suspects) != 1 || got.Suspects[0] != suspects[0] {
		t.Fatalf("labels = %s, want opening key and suspect", data)
	}
	// The sdktest loader, which rejects unknown label fields, reads them back.
	loaded, err := sdktest.LoadCorpusMetadata(filepath.Dir(filepath.Join(root, labelsPath)))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(loaded.OpeningKeys) != 1 || len(loaded.Suspects) != 1 {
		t.Fatalf("loaded = %+v", loaded)
	}
}

// Cases written before the incident labels existed stay valid unchanged.
func TestRepeatCorpusWithoutContextKeepsLabelsMinimal(t *testing.T) {
	root, captures := t.TempDir(), t.TempDir()
	prior, _ := json.Marshal(map[string]any{"root_cause_program_ids": []string{"fix"}, "bindings": component("api")})
	writeCaptureFile(t, filepath.Join(root, layout.CorpusDir, "earlier", "labels.json"), string(prior))
	for _, name := range []string{"fixed.json", "open@detect.json"} {
		writeCaptureFile(t, filepath.Join(captures, name), "{}")
	}
	writeCaptureFile(t, filepath.Join(captures, "open@detect-recordings.json"), "[]")
	paths, err := RecordRepeatCorpus(root, captures, "incident-1", map[string][]program.Bindings{"fix": {component("front")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if filepath.Base(path) != "labels.json" {
			continue
		}
		data, _ := os.ReadFile(filepath.Join(root, path))
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(data, &raw)
		if _, ok := raw["opening_keys"]; ok {
			t.Fatalf("labels without context gained opening_keys: %s", data)
		}
	}
}
