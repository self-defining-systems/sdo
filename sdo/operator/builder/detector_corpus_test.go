package builder

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/sdktest"
)

// The positive program binding is valid, but the independent detector inspects all
// Services. An unlabelled peer with no pods must not be skipped by save-time checks.
func TestProgramDetectorCorpusChecksEveryLocatedBinding(t *testing.T) {
	root := programFixture(t)
	detectorPath := filepath.Join(root, ".sdo/diagnostics/detectors/incidents/missing-configmap/detector.go")
	source, err := os.ReadFile(detectorPath)
	if err != nil {
		t.Fatal(err)
	}
	sourceText := string(source)
	start := strings.Index(sourceText, "func (Detector) Detect(")
	if start < 0 {
		t.Fatal("missing detector entry point")
	}
	sourceText = sourceText[:start] + `func (Detector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
 for _, service := range view.Services() { if service.Name == "unlabelled-peer" { return nil, fmt.Errorf("unlabelled-peer Pod observation unavailable") } }
 return nil, nil
 }
`
	// Retain constructor metadata; its declared playbook is now the executable program.
	sourceText = strings.ReplaceAll(sourceText, `const playbookPath = "missing-configmap"`, `const playbookPath = "service-targetport-mismatch"`)
	if err := os.WriteFile(detectorPath, []byte(sourceText), 0644); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(filepath.Dir(detectorPath), "manifest.yaml")
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(strings.ReplaceAll(string(data), "  - missing-configmap\n", "  - service-targetport-mismatch\n")), 0644); err != nil {
		t.Fatal(err)
	}
	// Add an unlabelled Service without changing the program's positive binding.
	for _, phase := range []string{"open", "fixed", "healthy"} {
		path := filepath.Join(root, ".sdo/corpus/targetport-mismatch", phase+".json")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot sdktest.Snapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		extra := snapshot.ServiceList[0].DeepCopy()
		extra.Name = "unlabelled-peer"
		extra.Spec.Selector = map[string]string{"app": "no-pods"}
		snapshot.ServiceList = append(snapshot.ServiceList, *extra)
		raw, err = json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	// Old hand-authored detector unit tests target the previous predicate.
	if err := os.Remove(filepath.Join(filepath.Dir(detectorPath), "detector_test.go")); err != nil {
		t.Fatal(err)
	}
	workspace, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	if err := workspace.Check(context.Background()); err == nil || !strings.Contains(err.Error(), "detector.corpus-observation") {
		t.Fatalf("unlabelled locator Unknown escaped complete memory check: %v", err)
	}
	// A complete locator whose bindings are all observable remains valid.
	for _, phase := range []string{"open", "fixed", "healthy"} {
		path := filepath.Join(root, ".sdo/corpus/targetport-mismatch", phase+".json")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot sdktest.Snapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			t.Fatal(err)
		}
		snapshot.ServiceList = snapshot.ServiceList[:len(snapshot.ServiceList)-1]
		raw, err = json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
	complete, err := buildProgramFixture(t, root)
	if err != nil {
		t.Fatal(err)
	}
	defer complete.Close()
	if err := complete.Check(context.Background()); err != nil {
		t.Fatalf("complete locator evidence rejected: %v", err)
	}

}
