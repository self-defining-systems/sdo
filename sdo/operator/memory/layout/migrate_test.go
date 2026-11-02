package layout

import (
	"os"
	"path/filepath"
	"strings"
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
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "3\n" {
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
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "3\n" {
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
		"future schema": func(root string) { writeFile(t, root, ".sdo/schema-version", "4\n") },
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

const pathManifest = `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: cache
constructor: New
class: incident
owner: responder
watches:
  - apiVersion: v1
    kind: Pod
interval: 10s
persistence:
  firing: 1
  clearing: 2
batching:
  severity: warn
  debounce: "0"
possiblePlaybooks:
  - .sdo/playbooks/p1/README.md
originatingIncident: inc-1
originatingCommit: abc
`

const pathDetector = `package cache

// Surfaces .sdo/playbooks/p1/README.md when the cache stalls.
const playbookPath = ".sdo/playbooks/p1/README.md"

var others = []string{playbookPath, ` + "`.sdo/playbooks/p2/README.md`" + `}

func hint() string { return "run .sdo/playbooks/p1/scripts/fix.sh to repair" }
`

func writeVersion2Repo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, ".sdo/schema-version", "2\n")
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", pathManifest)
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", pathDetector)
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector_test.go", "package cache\n\nconst fixture = \".sdo/playbooks/p2/README.md\"\n")
	return root
}

func TestMigrateVersion3RewritesPlaybookPathsInDetectorSourceToIDs(t *testing.T) {
	root := writeVersion2Repo(t)
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v", changed, err)
	}
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "3\n" {
		t.Errorf("schema-version = %q", version)
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(root, DetectorDir(ClassIncident, "cache"), name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	source := read("detector.go")
	for _, want := range []string{`const playbookPath = "p1"`, "[]string{playbookPath, `p2`}", "// Surfaces p1 when", `"run script:p1/fix.sh to repair"`} {
		if !strings.Contains(source, want) {
			t.Errorf("detector.go lacks %q:\n%s", want, source)
		}
	}
	if test := read("detector_test.go"); !strings.Contains(test, `const fixture = "p2"`) {
		t.Errorf("detector_test.go = %q", test)
	}
	for _, name := range []string{"detector.go", "detector_test.go", ManifestFileName} {
		if strings.Contains(read(name), PhysicalPathMarker) {
			t.Errorf("%s still names a physical path", name)
		}
	}
	manifest, err := ParseDetectorManifest([]byte(read(ManifestFileName)))
	if err != nil || len(manifest.PossiblePlaybooks) != 1 || manifest.PossiblePlaybooks[0] != "p1" {
		t.Fatalf("manifest = %+v, %v", manifest, err)
	}
	if again, err := Migrate(root); err != nil || again {
		t.Fatalf("second Migrate = %v, %v", again, err)
	}
}

func TestMigrateVersion3LeavesLogicalSourceUntouched(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, ".sdo/schema-version", "2\n")
	manifest := strings.Replace(pathManifest, ".sdo/playbooks/p1/README.md", "p1", 1)
	source := "package cache\n\nconst playbook = \"p1\"\n"
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", manifest)
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", source)
	if _, err := Migrate(root); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, DetectorDir(ClassIncident, "cache"), "detector.go")); string(got) != source {
		t.Errorf("source changed: %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, DetectorDir(ClassIncident, "cache"), ManifestFileName)); string(got) != manifest {
		t.Errorf("manifest changed: %q", got)
	}
}

func TestMigrateVersion1ReachesVersion3WithLogicalPlaybooks(t *testing.T) {
	root := writeLegacyRepo(t)
	writeFile(t, root, ".sdo/diagnostics/manifest.yaml", strings.ReplaceAll(legacyManifest, "possiblePlaybooks: [p1]", "possiblePlaybooks: [.sdo/playbooks/p1/README.md]"))
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/detector.go", "package cache\n\nconst p = \".sdo/playbooks/p1/README.md\"\n")
	if _, err := Migrate(root); err != nil {
		t.Fatal(err)
	}
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "3\n" {
		t.Errorf("schema-version = %q", version)
	}
	detectors, err := ReadDetectors(filepath.Join(root, DiagnosticsDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, detector := range detectors {
		if got := detector.Manifest.PossiblePlaybooks; len(got) != 1 || got[0] != "p1" {
			t.Errorf("%s possiblePlaybooks = %v", detector.Manifest.ID, got)
		}
		for _, file := range detector.Files {
			if strings.Contains(string(file.Content), PhysicalPathMarker) {
				t.Errorf("%s/%s still names a physical path", detector.Manifest.ID, file.Name)
			}
		}
	}
}

func TestMigrateVersion3RefusesUnreadableManifestsWithoutChangingTheRepository(t *testing.T) {
	root := writeVersion2Repo(t)
	writeFile(t, root, ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml", "not: [valid")
	if _, err := Migrate(root); err == nil {
		t.Fatal("Migrate accepted an unreadable manifest")
	}
	if got, _ := os.ReadFile(filepath.Join(root, DetectorDir(ClassIncident, "cache"), "detector.go")); string(got) != pathDetector {
		t.Errorf("source changed despite the failed migration: %q", got)
	}
	if version, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(version) != "2\n" {
		t.Errorf("schema-version = %q", version)
	}
}
