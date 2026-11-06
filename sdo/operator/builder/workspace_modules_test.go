package builder

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// operatorTree is the real sdo/operator source tree this package lives in.
func operatorTree(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestWorkspaceModuleReplacesEveryOperatorModule(t *testing.T) {
	root := t.TempDir()
	for _, module := range []string{"sdk", "core", "runtime", "contracts", "telemetry", "added-later"} {
		if err := os.MkdirAll(filepath.Join(root, module), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, module, "go.mod"), []byte("module x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := workspaceModule("module app\n\ngo 1.26.0\n", "app", root)
	if err != nil {
		t.Fatal(err)
	}
	for _, module := range []string{"sdk", "core", "runtime", "contracts", "telemetry", "added-later"} {
		want := "replace sdo.dev/operator/" + module + " => " + filepath.Join(root, module)
		if !strings.Contains(got, want) {
			t.Errorf("synthesized go.mod lacks %q:\n%s", want, got)
		}
	}
}

// Every nested module of the real tree must be one a synthesized build can
// resolve, and must appear in the list the CI script loops over, so adding a
// module cannot break nested builds again.
func TestEveryOperatorModuleIsInEachBuildList(t *testing.T) {
	root := operatorTree(t)
	repo := filepath.Dir(filepath.Dir(root))
	got, err := workspaceModule("module app\n\ngo 1.26.0\n", "app", root)
	if err != nil {
		t.Fatal(err)
	}
	ci, err := os.ReadFile(filepath.Join(repo, "scripts", "test_go_ci.sh"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, entry := range entries {
		if _, err := os.Stat(filepath.Join(root, entry.Name(), "go.mod")); err != nil {
			continue
		}
		found++
		name := entry.Name()
		if !strings.Contains(got, "replace sdo.dev/operator/"+name+" => ") {
			t.Errorf("module %s is not replaced in synthesized builds", name)
		}
		if !regexp.MustCompile(`(?m)sdo/operator/` + name + `(\s|$)`).Match(ci) {
			t.Errorf("module %s missing from scripts/test_go_ci.sh", name)
		}
	}
	if found == 0 {
		t.Fatal("no operator modules found")
	}
}
