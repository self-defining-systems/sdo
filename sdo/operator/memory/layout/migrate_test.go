package layout

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

const legacyManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
detectors:
  - id: health
    package: ./detectors/health/objective
    constructor: New
    class: health
    owner: health_judge
    watches:
      - apiVersion: v1
        kind: Pod
    interval: 10s
    persistence: {firing: 2, clearing: 2}
    batching: {severity: critical, debounce: 500ms}
    possiblePlaybooks: [p1]
    originatingCommit: boot
  - id: cache
    package: ./detectors/incidents/cache
    constructor: New
    class: incident
    owner: responder
    watches:
      - apiVersion: v1
        kind: Pod
    interval: 10s
    persistence: {firing: 1, clearing: 2}
    batching: {severity: warn, debounce: "0"}
    possiblePlaybooks: [p1]
    originatingIncident: inc-1
    originatingCommit: abc
`

func writeLegacyRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, ".sdo/schema-version", "1\n")
	writeFile(t, root, ".sdo/diagnostics/manifest.yaml", legacyManifest)
	writeFile(t, root, ".sdo/diagnostics/detectors/health/objective/detector.go", "package objective\n")
	writeFile(t, root, ".sdo/diagnostics/detectors/health/objective/detector_test.go", "package objective\n")
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n")
	return root
}

func TestMigrateSplitsLegacyManifestIntoPerDetectorManifests(t *testing.T) {
	root := writeLegacyRepo(t)
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(root, LegacyDetectorManifestFile)); !os.IsNotExist(err) {
		t.Errorf("legacy manifest remains: %v", err)
	}
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "2\n" {
		t.Errorf("schema-version = %q", version)
	}
	// The detector ID differs from its legacy package directory, so the
	// package moves to the canonical <id> directory.
	if _, err := os.Stat(filepath.Join(root, DetectorDir(ClassHealth, "health"), "detector_test.go")); err != nil {
		t.Errorf("health detector files were not moved: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".sdo/diagnostics/detectors/health/objective")); !os.IsNotExist(err) {
		t.Errorf("old package directory remains: %v", err)
	}
	writeFile(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\nGoal\n")
	writeFile(t, root, ".sdo/arch.md", "---\nschema_version: 1\n---\nArch\n")
	tree, err := ReadTree(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(tree.Detectors) != 2 {
		t.Fatalf("detectors = %#v", tree.Detectors)
	}
	var legacy struct {
		Detectors []DetectorManifest `yaml:"detectors"`
	}
	if err := yaml.Unmarshal([]byte(legacyManifest), &legacy); err != nil {
		t.Fatal(err)
	}
	want := map[string]DetectorManifest{}
	for _, d := range legacy.Detectors {
		want[d.ID] = d
	}
	for _, detector := range tree.Detectors {
		expected := want[detector.Manifest.ID]
		if err := detector.Manifest.Validate(); err != nil {
			t.Error(err)
		}
		if detector.Manifest.Interval != expected.Interval || detector.Manifest.OriginatingIncident != expected.OriginatingIncident || detector.Manifest.OriginatingCommit != expected.OriginatingCommit || detector.Manifest.Persistence != expected.Persistence || detector.Manifest.Batching != expected.Batching {
			t.Errorf("manifest changed by migration: %#v want %#v", detector.Manifest, expected)
		}
	}
}

func TestMigrateIsIdempotentAndLeavesCurrentRepositoriesAlone(t *testing.T) {
	root := writeLegacyRepo(t)
	if _, err := Migrate(root); err != nil {
		t.Fatal(err)
	}
	changed, err := Migrate(root)
	if err != nil || changed {
		t.Fatalf("second Migrate = %v, %v", changed, err)
	}
}

func TestMigrateFreshRepositoryOnlyStampsSchema(t *testing.T) {
	root := t.TempDir()
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v", changed, err)
	}
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "2\n" {
		t.Errorf("schema-version = %q", version)
	}
}

func TestMigrateRejectsAmbiguousLegacyRepositories(t *testing.T) {
	for name, edit := range map[string]func(root string){
		"package outside owned directory": func(root string) {
			writeFile(t, root, ".sdo/diagnostics/manifest.yaml", `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
detectors:
  - {id: x, package: ../outside, class: health, owner: health_judge}
`)
		},
		"destination occupied": func(root string) {
			writeFile(t, root, ".sdo/diagnostics/detectors/health/health/detector.go", "package other\n")
		},
		"future schema": func(root string) { writeFile(t, root, ".sdo/schema-version", "3\n") },
	} {
		t.Run(name, func(t *testing.T) {
			root := writeLegacyRepo(t)
			edit(root)
			if _, err := Migrate(root); err == nil {
				t.Fatal("Migrate accepted an ambiguous repository")
			}
		})
	}
}
