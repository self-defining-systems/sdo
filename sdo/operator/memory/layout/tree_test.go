package layout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, root, relative, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func sampleManifest(class Class, id string) DetectorManifest {
	owner, incident := "health_judge", ""
	if class == ClassIncident {
		owner, incident = "responder", "incident-1"
	}
	return DetectorManifest{
		APIVersion: APIVersion, Kind: DetectorManifestKind, SDKVersion: SDKVersion,
		ID: id, Constructor: "New", Class: string(class), Owner: owner,
		Watches:  []Watch{{APIVersion: "v1", Kind: "Pod"}},
		Interval: "10s", Persistence: Persistence{Firing: 1, Clearing: 2}, Batching: Batching{Severity: "warn", Debounce: "0"},
		PossiblePlaybooks: []string{"p1"}, OriginatingIncident: incident, OriginatingCommit: "c1",
	}
}

func writeTree(t *testing.T, root string) {
	t.Helper()
	writeFile(t, root, ".sdo/schema-version", "2\n")
	writeFile(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\n---\n# Goal\n")
	writeFile(t, root, ".sdo/arch.md", "---\nschema_version: 1\n---\nArchitecture\n")
	writeFile(t, root, ".sdo/playbooks/README.md", "# Operational playbooks\n")
	writeFile(t, root, ".sdo/playbooks/p1/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: p1\noriginating_incident: i\n---\nBody <ROLE_X>\n")
	writeFile(t, root, ".sdo/playbooks/p1/scripts/fix.sh", "#!/bin/sh\ntrue\n")
	for _, class := range []Class{ClassHealth, ClassIncident} {
		if err := WriteDetector(root, Detector{Class: class, Manifest: sampleManifest(class, "d-"+string(class)), Files: []File{{Name: "detector.go", Content: []byte("package d\n")}}}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadTreeParsesEveryKind(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root)
	tree, err := ReadTree(root)
	if err != nil {
		t.Fatal(err)
	}
	if tree.SchemaVersion != 2 {
		t.Errorf("schema version = %d", tree.SchemaVersion)
	}
	if tree.Goal.Meta["owner"] != "human" || !strings.Contains(tree.Goal.Body, "# Goal") {
		t.Errorf("goal = %#v", tree.Goal)
	}
	if tree.Architecture == nil || tree.PlaybookIndex == "" {
		t.Errorf("architecture or playbook index missing: %#v", tree)
	}
	if len(tree.Detectors) != 2 || tree.Detectors[0].Class != ClassHealth || tree.Detectors[1].Class != ClassIncident {
		t.Fatalf("detectors = %#v", tree.Detectors)
	}
	incident := tree.Detectors[1]
	if incident.Manifest.OriginatingIncident != "incident-1" || incident.Package() != "detectors/incidents/d-incident" || len(incident.Files) != 1 || incident.Files[0].Name != "detector.go" {
		t.Errorf("incident detector = %#v", incident)
	}
	if len(tree.Playbooks) != 1 || tree.Playbooks[0].ID != "p1" || tree.Playbooks[0].Document.Meta["fault_class"] != "p1" || len(tree.Playbooks[0].Scripts) != 1 || tree.Playbooks[0].Scripts[0].Name != "fix.sh" {
		t.Errorf("playbooks = %#v", tree.Playbooks)
	}
}

func TestReadTreeRejectsUnsafeOrUnsupportedMemory(t *testing.T) {
	for name, mutate := range map[string]func(root string){
		"old schema":      func(root string) { writeFile(t, root, ".sdo/schema-version", "1\n") },
		"legacy manifest": func(root string) { writeFile(t, root, ".sdo/diagnostics/manifest.yaml", "detectors: []\n") },
		"unknown file":    func(root string) { writeFile(t, root, ".sdo/stray.txt", "x") },
		"id mismatch": func(root string) {
			writeFile(t, root, ".sdo/diagnostics/detectors/health/other/manifest.yaml", "id: d-health\n")
		},
		"class mismatch": func(root string) {
			writeManifestFor(t, root, ClassHealth, "bad", func(m *DetectorManifest) { m.Class = "incident" })
		},
		"symlink": func(root string) { _ = os.Symlink("goal.md", filepath.Join(root, ".sdo/link.md")) },
		"unknown manifest field": func(root string) {
			writeFile(t, root, ".sdo/diagnostics/detectors/health/x/manifest.yaml", "apiVersion: sdo.dev/v1alpha1\nbogus: 1\n")
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writeTree(t, root)
			mutate(root)
			if _, err := ReadTree(root); err == nil {
				t.Fatal("ReadTree accepted invalid memory")
			}
		})
	}
}

func writeManifestFor(t *testing.T, root string, class Class, id string, edit func(*DetectorManifest)) {
	t.Helper()
	manifest := sampleManifest(class, id)
	edit(&manifest)
	if err := WriteDetector(root, Detector{Class: class, Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
}

func TestDetectorManifestValidate(t *testing.T) {
	valid := sampleManifest(ClassIncident, "d1")
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*DetectorManifest){
		"bad id":        func(m *DetectorManifest) { m.ID = "Bad" },
		"wrong owner":   func(m *DetectorManifest) { m.Owner = "health_judge" },
		"no origin":     func(m *DetectorManifest) { m.OriginatingIncident = "" },
		"no commit":     func(m *DetectorManifest) { m.OriginatingCommit = "" },
		"bad interval":  func(m *DetectorManifest) { m.Interval = "soon" },
		"no watches":    func(m *DetectorManifest) { m.Watches = nil },
		"dup watches":   func(m *DetectorManifest) { m.Watches = append(m.Watches, m.Watches[0]) },
		"bad severity":  func(m *DetectorManifest) { m.Batching.Severity = "loud" },
		"bad debounce":  func(m *DetectorManifest) { m.Batching.Debounce = "-1s" },
		"zero firing":   func(m *DetectorManifest) { m.Persistence.Firing = 0 },
		"bad playbook":  func(m *DetectorManifest) { m.PossiblePlaybooks = []string{"../x"} },
		"dup playbook":  func(m *DetectorManifest) { m.PossiblePlaybooks = []string{"a", "a"} },
		"bad ctor":      func(m *DetectorManifest) { m.Constructor = "1x" },
		"bad sdk":       func(m *DetectorManifest) { m.SDKVersion = "v9" },
		"unknown class": func(m *DetectorManifest) { m.Class = "other" },
		"nil playbooks": func(m *DetectorManifest) { m.PossiblePlaybooks = nil },
	} {
		manifest := sampleManifest(ClassIncident, "d1")
		edit(&manifest)
		if err := manifest.Validate(); err == nil {
			t.Errorf("%s: invalid manifest accepted", name)
		}
	}
}

func TestParseDetectorManifestIsStrict(t *testing.T) {
	data, err := MarshalDetectorManifest(sampleManifest(ClassHealth, "d1"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseDetectorManifest(data)
	if err != nil || parsed.ID != "d1" {
		t.Fatalf("round trip = %#v, %v", parsed, err)
	}
	if _, err := ParseDetectorManifest(append(data, []byte("extra: 1\n")...)); err == nil {
		t.Error("unknown field accepted")
	}
	if _, err := ParseDetectorManifest([]byte("id: a\nid: b\n")); err == nil {
		t.Error("duplicate key accepted")
	}
}

func TestParseDocument(t *testing.T) {
	doc, err := ParseDocument([]byte("---\nowner: human\nschema_version: 1\n---\nbody\n"))
	if err != nil || doc.Meta["owner"] != "human" || strings.TrimSpace(doc.Body) != "body" {
		t.Fatalf("doc = %#v, %v", doc, err)
	}
	for _, bad := range []string{"no front matter", "---\nowner: [\n---\nx", "---\nschema_version: 1\n---\n  \n"} {
		if _, err := ParseDocument([]byte(bad)); err == nil {
			t.Errorf("ParseDocument(%q) succeeded", bad)
		}
	}
}
