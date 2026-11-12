package layout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if got, _ := os.ReadFile(filepath.Join(root, SchemaVersionFile)); string(got) != "7\n" {
		t.Fatalf("schema-version = %q, want 7", got)
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
