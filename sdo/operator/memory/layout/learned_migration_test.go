package layout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/sdktest"
)

func writeFixtureFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

const incidentManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: cache
constructor: New
class: incident
owner: responder
watches:
    - apiVersion: v1
      kind: Pod
interval: 30s
persistence:
    firing: 1
    clearing: 1
batching:
    severity: warn
    debounce: "0"
possiblePlaybooks: []
originatingIncident: inc-1
originatingCommit: abc
`

// Schema 13 renames the incident detector class to learned. A schema 12
// repository moves detectors/incidents to detectors/learned, and the manifest
// class and the class named in detector source follow; health detectors and
// every other file stay as they were.
func TestMigrateToSchema13RenamesIncidentClassToLearned(t *testing.T) {
	root := t.TempDir()
	healthSource := "package health\n\n// DetectorClassHealth stays.\nvar _ = sdk.DetectorClassHealth\n"
	writeFixtureFile(t, root, SchemaVersionFile, "12\n")
	writeFixtureFile(t, root, GoalFile, "Goal\n")
	writeFixtureFile(t, root, DetectorsDir+"/health/health/detector.go", healthSource)
	writeFixtureFile(t, root, DetectorsDir+"/incidents/cache/manifest.yaml", incidentManifest)
	writeFixtureFile(t, root, DetectorsDir+"/incidents/cache/detector.go", "package cache\n\nvar a = sdk.DetectorClassIncident\nvar b = sdk.DetectorClass(\"incident\")\n")
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v; want changed", changed, err)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(DetectorsDir+"/incidents"))); !os.IsNotExist(err) {
		t.Fatalf("the incidents directory remains: %v", err)
	}
	manifest, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(DetectorManifestPath(ClassLearned, "cache"))))
	if err != nil || !strings.Contains(string(manifest), "class: learned\n") || strings.Contains(string(manifest), "incident\n") {
		t.Fatalf("manifest = %q, %v", manifest, err)
	}
	source, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(DetectorDir(ClassLearned, "cache")+"/detector.go")))
	if got, want := string(source), "package cache\n\nvar a = sdk.DetectorClassLearned\nvar b = sdk.DetectorClass(\"learned\")\n"; got != want {
		t.Fatalf("source = %q, want %q", got, want)
	}
	if got, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(DetectorDir(ClassHealth, "health")+"/detector.go"))); string(got) != healthSource {
		t.Fatalf("health detector changed: %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "14\n" {
		t.Fatalf("schema-version = %q, want 14", got)
	}
	if changed, err := Migrate(root); err != nil || changed {
		t.Fatalf("second Migrate = %v, %v; want unchanged", changed, err)
	}
}

// A repository already holding detectors/learned beside detectors/incidents is
// ambiguous: migration refuses before changing any file.
func TestMigrateToSchema13RefusesBothDirectories(t *testing.T) {
	root := t.TempDir()
	writeFixtureFile(t, root, SchemaVersionFile, "12\n")
	writeFixtureFile(t, root, DetectorsDir+"/incidents/cache/manifest.yaml", incidentManifest)
	writeFixtureFile(t, root, DetectorsDir+"/learned/other/manifest.yaml", incidentManifest)
	if changed, err := Migrate(root); err == nil || changed {
		t.Fatalf("Migrate = %v, %v; want an error", changed, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "12\n" {
		t.Fatalf("a refused migration stamped the schema: %q", got)
	}
	if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(DetectorsDir+"/incidents/cache/manifest.yaml"))); err != nil {
		t.Fatalf("a refused migration moved a detector: %v", err)
	}
}

func TestMigrateSchema13PreservesLegacyCorpusLabels(t *testing.T) {
	root := t.TempDir()
	label := ".sdo/corpus/synthetic/labels.json"
	content := `{"script_origins":{"digest":"receipt"}}`
	writeFixtureFile(t, root, SchemaVersionFile, "13\n")
	writeFixtureFile(t, root, label, content)
	if changed, err := Migrate(root); err != nil || !changed {
		t.Fatalf("migration = %v, %v", changed, err)
	}
	if version, err := readSchemaVersion(root); err != nil || version != 14 {
		t.Fatalf("version = %d, %v", version, err)
	}
	if data, err := os.ReadFile(filepath.Join(root, label)); err != nil || string(data) != content {
		t.Fatalf("migration changed immutable corpus labels: %v", err)
	}
	if _, err := sdktest.LoadCorpusMetadata(filepath.Join(root, filepath.Dir(label))); err != nil {
		t.Fatalf("migrated labels are unreadable: %v", err)
	}
}
