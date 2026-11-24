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

// A program's first case needs no healthy peer: it is written from the incident
// alone, role-less or not, and its labels carry no peer. Replay then
// diagnoses the healthy capture on the case's own bindings.
func TestRecordCorpusWritesAProgramsFirstCaseWithoutAPeer(t *testing.T) {
	for name, binding := range map[string]program.Bindings{"role-less": {}, "one role": component("front")} {
		t.Run(name, func(t *testing.T) {
			root, captures := t.TempDir(), t.TempDir()
			for _, file := range []string{"fixed.json", "open@detect.json"} {
				writeCaptureFile(t, filepath.Join(captures, file), "{}")
			}
			writeCaptureFile(t, filepath.Join(captures, "open@detect-recordings.json"), "[]")
			paths, err := RecordCorpus(root, captures, "incident-1", map[string][]program.Bindings{"fresh": {binding}}, nil, CaseContext{})
			if err != nil || len(paths) == 0 {
				t.Fatalf("RecordCorpus = %v, %v; a new program's first case must not need a peer", paths, err)
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
			loaded, err := sdktest.LoadCorpusMetadata(filepath.Dir(filepath.Join(root, labelsPath)))
			if err != nil {
				t.Fatalf("load: %v", err)
			}
			if len(loaded.PeerBindings.Objects) != 0 || len(loaded.PeerBindings.Values) != 0 {
				t.Fatalf("a first case invents no peer: %s", data)
			}
			if len(loaded.RootCauseProgramIDs) != 1 || loaded.RootCauseProgramIDs[0] != "fresh" || loaded.Provenance.IncidentID != "incident-1" {
				t.Fatalf("loaded = %+v", loaded)
			}
		})
	}
}

func TestGeneratedCorpusOmitsOnlyContaminatedRecordings(t *testing.T) {
	for _, negative := range []bool{false, true} {
		name := "positive"
		if negative {
			name = "fixed-no-match"
		}
		t.Run(name, func(t *testing.T) {
			root, captures := t.TempDir(), t.TempDir()
			for _, name := range []string{"fixed.json", "open@detect.json", "open@pre-apply.json"} {
				writeCaptureFile(t, filepath.Join(captures, name), "{}")
			}
			canary := "glpat-" + "synthetic_token_for_test_only"
			writeCaptureFile(t, filepath.Join(captures, "open@detect-recordings.json"), `[{"result":"`+canary+`"}]`)
			const clean = `[{"method":"Log"}]`
			writeCaptureFile(t, filepath.Join(captures, "open@pre-apply-recordings.json"), clean)
			instances := map[string][]program.Bindings{"dirty": {{}}, "clean": {{}}}
			origins := map[string]string{CapturedInstanceKey("clean", program.Bindings{}): "pre-apply"}
			record := func() ([]string, error) {
				if negative {
					return RecordFixedNoMatchCorpus(root, captures, "incident-1", instances, origins)
				}
				return RecordCorpus(root, captures, "incident-1", instances, origins, CaseContext{})
			}
			paths, err := record()
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]int{}
			var cleanPath string
			for _, path := range paths {
				if filepath.Base(path) != "recordings.json" {
					continue
				}
				raw, err := os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
				got[string(raw)]++
				if string(raw) == clean {
					cleanPath = path
				}
			}
			if len(got) != 2 || got["[]"] != 1 || got[clean] != 1 {
				t.Fatal("expected both cases with only the contaminated recordings discarded")
			}
			if err := ValidateCapturedCorpusDirectory(captures, root, "incident-1", paths); err != nil {
				t.Fatalf("generated cases failed provenance: %v", err)
			}
			writeCaptureFile(t, filepath.Join(root, cleanPath), `[{"method":"Other"}]`)
			if err := ValidateCapturedCorpusDirectory(captures, root, "incident-1", paths); err == nil {
				t.Fatal("changed clean recordings passed provenance")
			}
			contaminated, err := json.Marshal(sdktest.Snapshot{NamespaceName: canary})
			if err != nil {
				t.Fatal(err)
			}
			writeCaptureFile(t, filepath.Join(captures, "fixed.json"), string(contaminated))
			if paths, err := record(); err != nil || len(paths) != 0 {
				t.Fatalf("contaminated fixed snapshot yielded %d files, err = %v", len(paths), err)
			}
		})
	}
}
