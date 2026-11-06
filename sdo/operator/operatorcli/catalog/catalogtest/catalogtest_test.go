package catalogtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fixture builds a family with one implementation (pkg "gitpoll" for ID "git")
// whose test calls the suite and whose docs row exists.
func fixture(t *testing.T) (Spec, string) {
	root := t.TempDir()
	write(t, root, "family/gitpoll/gitpoll_test.go", "package gitpoll\n\nfunc TestX() { suite.Run(t, c, ID, cfg) }\n")
	write(t, root, "family/suitetest/x.go", "package suitetest\n")
	write(t, root, "docs.md", "| `sdo/operator/family/gitpoll/` | x |\n")
	return Spec{
		IDs: []string{"git"}, Packages: map[string]string{"git": "gitpoll"},
		FamilyDir: filepath.Join(root, "family"), DocsPath: "sdo/operator/family/", DocsFile: filepath.Join(root, "docs.md"),
		SupportDirs: []string{"suitetest"}, Suite: "suite.Run",
	}, root
}

func TestCompleteFamilyHasNoProblems(t *testing.T) {
	spec, _ := fixture(t)
	if problems, err := Problems(spec); err != nil || len(problems) != 0 {
		t.Fatalf("problems = %v, %v", problems, err)
	}
}

func TestProblemsNamesEachGap(t *testing.T) {
	cases := map[string]struct {
		mutate func(*testing.T, *Spec, string)
		want   string
	}{
		"id without table entry": {func(_ *testing.T, s *Spec, _ string) { s.Packages = nil }, "has no entry for it"},
		"stale table entry":      {func(_ *testing.T, s *Spec, _ string) { s.Packages["gone"] = "gone" }, "does not register"},
		"suite not called": {func(t *testing.T, _ *Spec, root string) {
			write(t, root, "family/gitpoll/gitpoll_test.go", "package gitpoll\n")
		}, "no test calling suite.Run"},
		"suite called with another ID": {func(t *testing.T, _ *Spec, root string) {
			write(t, root, "family/gitpoll/gitpoll_test.go", "package gitpoll\n\nfunc TestX() { suite.Run(t, c, \"other\", cfg) }\n")
		}, "no test calling suite.Run"},
		"no docs row": {func(t *testing.T, _ *Spec, root string) { write(t, root, "docs.md", "") }, "no component-map row"},
		"unregistered implementation": {func(t *testing.T, _ *Spec, root string) {
			write(t, root, "family/orphan/o.go", "package orphan\n")
		}, "no registered ID maps to"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			spec, root := fixture(t)
			tc.mutate(t, &spec, root)
			problems, err := Problems(spec)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(problems, "\n"), tc.want) {
				t.Fatalf("problems = %v, want one containing %q", problems, tc.want)
			}
		})
	}
}
