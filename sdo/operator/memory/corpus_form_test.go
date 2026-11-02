package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// assertSnapshotsStoredOnce checks a written case holds exactly the named
// snapshot files, each in the objects-only form.
func assertSnapshotsStoredOnce(t *testing.T, dir string, snapshots ...string) {
	t.Helper()
	for _, name := range []string{"open.json", "fixed.json", "healthy.json"} {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if !slices.Contains(snapshots, name) {
			if !os.IsNotExist(err) {
				t.Fatalf("%s stores a copy of fixed.json", name)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		if string(fields["object_snapshot_version"]) != "2" {
			t.Fatalf("%s is not objects-only: version %s", name, fields["object_snapshot_version"])
		}
	}
}

// A capture written by an earlier controller (typed lists beside objects) is
// copied in the objects-only form and still matches its trusted capture.
func TestCorpusWritersCompactEarlierCaptureForm(t *testing.T) {
	root, captures := t.TempDir(), t.TempDir()
	legacy := func(raw []byte) []byte {
		var snapshot sdktest.Snapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		fields["object_snapshot_version"] = 1
		fields["services"] = snapshot.ServiceList
		encoded, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	before := legacy(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"target"}}]}`))
	fixed := legacy(capturefixture.FromView(t, `{"namespace":"app","services":[{"metadata":{"namespace":"app","name":"target"}},{"metadata":{"namespace":"app","name":"peer"}}]}`))
	for name, data := range map[string][]byte{"open@detect.json": before, "fixed.json": fixed, "open@detect-recordings.json": []byte("[]")} {
		if err := os.WriteFile(filepath.Join(captures, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	prior := filepath.Join(root, ".sdo/corpus/prior")
	if err := os.MkdirAll(prior, 0755); err != nil {
		t.Fatal(err)
	}
	peer := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "peer"}}}
	labels, _ := json.Marshal(map[string]any{"root_cause_program_ids": []string{"routing"}, "peer_bindings": peer})
	if err := os.WriteFile(filepath.Join(prior, "labels.json"), labels, 0644); err != nil {
		t.Fatal(err)
	}
	bindings := map[string][]program.Bindings{"routing": {{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "target"}}}}}
	repeat, err := RecordRepeatCorpus(root, captures, "incident", bindings)
	if err != nil {
		t.Fatal(err)
	}
	negative, err := RecordFixedNoMatchCorpus(root, captures, "incident", bindings)
	if err != nil {
		t.Fatal(err)
	}
	for _, paths := range [][]string{repeat, negative} {
		dir := filepath.Join(root, filepath.Dir(paths[0]))
		if filepath.Base(dir) == filepath.Base(filepath.Dir(filepath.Join(root, negative[0]))) {
			assertSnapshotsStoredOnce(t, dir, "fixed.json")
		} else {
			assertSnapshotsStoredOnce(t, dir, "open.json", "fixed.json")
		}
		if err := ValidateCapturedCorpusDirectory(captures, root, "incident", paths); err != nil {
			t.Fatal(err)
		}
		if err := sdktest.ValidateCorpusProvenance(dir); err != nil {
			t.Fatal(err)
		}
		c, err := sdktest.LoadCorpusCase(dir)
		if err != nil || len(c.Fixed.Services()) != 2 || len(c.Healthy.Services()) != 2 {
			t.Fatalf("loaded case: %+v %v", c.Fixed.Services(), err)
		}
	}
}
