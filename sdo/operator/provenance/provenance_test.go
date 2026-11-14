package provenance

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSourceDigestFollowsOperatorSource(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"go.mod": "module x\n", "a/a.go": "package a\n"})
	first, err := SourceDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := SourceDigest(root); again != first {
		t.Fatalf("digest not stable: %s then %s", first, again)
	}
	writeTree(t, root, map[string]string{"a/a.go": "package a\n// changed\n"})
	changed, _ := SourceDigest(root)
	if changed == first {
		t.Fatal("changed operator source kept the same build ID")
	}
}

func TestSourceDigestIgnoresTestsAndFixtures(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"a/a.go": "package a\n"})
	first, _ := SourceDigest(root)
	writeTree(t, root, map[string]string{"a/a_test.go": "package a\n", "a/testdata/x.go": "package x\n", "notes.md": "x"})
	if again, _ := SourceDigest(root); again != first {
		t.Fatal("tests, fixtures and docs must not change the build ID")
	}
}

func TestSourceDigestIsRootIndependent(t *testing.T) {
	files := map[string]string{"go.mod": "module x\n", "a/a.go": "package a\n"}
	one, two := t.TempDir(), t.TempDir()
	writeTree(t, one, files)
	writeTree(t, two, files)
	a, _ := SourceDigest(one)
	b, _ := SourceDigest(two)
	if a != b {
		t.Fatalf("same source at two roots: %s != %s", a, b)
	}
}

func TestOperatorBuildHashesTheMountedSource(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"a/a.go": "package a\n"})
	first := OperatorBuild(root)
	if !strings.HasPrefix(first, "src-") {
		t.Fatalf("OperatorBuild = %q, want a source digest on the dev-mount path", first)
	}
	// A distinct root list is a distinct cache key; edit and ask again via a fresh key.
	writeTree(t, root, map[string]string{"a/a.go": "package a\n// edit\n"})
	if second := OperatorBuild(root, "edited"); second == first {
		t.Fatal("OperatorBuild did not change with the mounted source")
	}
}
