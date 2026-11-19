package layout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc"
)

func TestClassifyDeployment(t *testing.T) {
	for p, want := range map[string]struct {
		kind Kind
		id   string
	}{
		".sdo/deployment/plan.yaml":                       {KindDeploymentPlan, ""},
		".sdo/deployment/values/prod.yaml":                {KindDeploymentFile, "values"},
		".sdo/deployment/manifests/a/b/service.yaml":      {KindDeploymentFile, "manifests"},
		".sdo/deployment/build/Dockerfile":                {KindDeploymentFile, "build"},
		".sdo/deployment/overlays/telemetry/metrics.yaml": {KindDeploymentOverlay, "telemetry"},
		".sdo/deployment/patches/0001-fix-probe.patch":    {KindDeploymentPatch, "0001-fix-probe"},
		".sdo/deployment/patches/0042-a.patch":            {KindDeploymentPatch, "0042-a"},
	} {
		kind, id, ok := Classify(p)
		if !ok || kind != want.kind || id != want.id {
			t.Errorf("Classify(%q) = %q, %q, %v; want %q, %q", p, kind, id, ok, want.kind, want.id)
		}
	}
}

func TestClassifyDeploymentIsStrict(t *testing.T) {
	for _, p := range []string{
		".sdo/deployment", ".sdo/deployment/", ".sdo/deployment/plan.yaml/x", ".sdo/deployment/plan.yml",
		".sdo/deployment/values", ".sdo/deployment/other/x.yaml", ".sdo/deployment/notes.md",
		".sdo/deployment/overlays/telemetry", ".sdo/deployment/overlays/other/x.yaml", ".sdo/deployment/overlays/x.yaml",
		".sdo/deployment/patches", ".sdo/deployment/patches/fix.patch", ".sdo/deployment/patches/001-fix.patch",
		".sdo/deployment/patches/0001-Fix.patch", ".sdo/deployment/patches/0001-.patch", ".sdo/deployment/patches/0001-fix.diff",
		".sdo/deployment/patches/0001-fix.patch/x", ".sdo/deployment/patches/sub/0001-fix.patch",
		".sdo/deployment/patches/0001-" + strings.Repeat("a", 50) + ".patch",
		".sdo/deployment/values/../plan.yaml", ".sdo/rendered/lock.json", ".sdo/rendered/" + strings.Repeat("0", 64) + ".yaml",
	} {
		if kind, id, ok := Classify(p); ok {
			t.Errorf("Classify(%q) = %q, %q; want unclassified", p, kind, id)
		}
	}
}

func TestMigrateToSchema4OnlyStamps(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(SchemaVersionFile, "3\n")
	write(GoalFile, "Goal\n")
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v; want changed", changed, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "13\n" {
		t.Fatalf("schema-version = %q, want 13", got)
	}
	var files []string
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(root, p)
			files = append(files, filepath.ToSlash(rel))
		}
		return nil
	})
	if strings.Join(files, ",") != ".sdo/goal.md,.sdo/schema-version" {
		t.Fatalf("migration changed the file set: %v", files)
	}
	if changed, err := Migrate(root); err != nil || changed {
		t.Fatalf("second Migrate = %v, %v; want unchanged", changed, err)
	}
}

// A repository at schema 7 holds cases without the Go-written incident labels.
// Migration stamps it and leaves every case byte for byte, so they stay valid.
func TestMigrateToSchema8KeepsExistingCorpusCases(t *testing.T) {
	root := t.TempDir()
	labels := `{"root_cause_program_ids":["fix"],"bindings":{"objects":{}}}` + "\n"
	for rel, content := range map[string]string{SchemaVersionFile: "7\n", GoalFile: "Goal\n", CorpusDir + "/case-1/labels.json": labels} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v; want changed", changed, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "13\n" {
		t.Fatalf("schema-version = %q, want 13", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(CorpusDir+"/case-1/labels.json"))); string(got) != labels {
		t.Fatalf("an existing case changed: %q", got)
	}
}

// A repository at schema 10 may hold an outcome document that still carries the
// retired disabled_setup_detectors. Migration stamps it and leaves every outcome
// byte for byte; readers drop the retired field.
func TestMigrateToSchema11KeepsExistingOutcomes(t *testing.T) {
	root := t.TempDir()
	outcome := `{"incident_id":"i","disabled_setup_detectors":[{"detector_id":"learned"}]}` + "\n"
	const path = ".sdo/outcomes/0000.json"
	for rel, content := range map[string]string{SchemaVersionFile: "10\n", GoalFile: "Goal\n", path: outcome} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v; want changed", changed, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "13\n" {
		t.Fatalf("schema-version = %q, want 13", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(path))); string(got) != outcome {
		t.Fatalf("an existing outcome changed: %q", got)
	}
}

// A repository at schema 11 holds recordings whose failed reads carry no class.
// Migration stamps it and leaves every recording byte for byte: a recording
// with no error_class reads as unavailable.
func TestMigrateToSchema12KeepsExistingRecordings(t *testing.T) {
	root := t.TempDir()
	recordings := `[{"method":"DiffAgainstSource","arguments":[],"result":{},"error":"object not captured"}]` + "\n"
	for rel, content := range map[string]string{SchemaVersionFile: "11\n", GoalFile: "Goal\n", CorpusDir + "/case-1/recordings.json": recordings} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := Migrate(root)
	if err != nil || !changed {
		t.Fatalf("Migrate = %v, %v; want changed", changed, err)
	}
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "13\n" {
		t.Fatalf("schema-version = %q, want 13", got)
	}
	if got, _ := os.ReadFile(filepath.Join(root, filepath.FromSlash(CorpusDir+"/case-1/recordings.json"))); string(got) != recordings {
		t.Fatalf("an existing recording changed: %q", got)
	}
}

// The build-input rule's authored Dockerfile directory is this layout's "build" deployment files.
func TestAuthoredDockerfilesAreBuildDeploymentFiles(t *testing.T) {
	if imagesrc.AuthoredDockerfileDir != DeploymentDir+"/build" {
		t.Fatalf("imagesrc.AuthoredDockerfileDir = %q, want %q", imagesrc.AuthoredDockerfileDir, DeploymentDir+"/build")
	}
	if kind, id, ok := Classify(imagesrc.AuthoredDockerfileDir + "/api/Dockerfile"); !ok || kind != KindDeploymentFile || id != "build" {
		t.Fatalf("Classify = %q, %q, %v; want a build deployment file", kind, id, ok)
	}
}
