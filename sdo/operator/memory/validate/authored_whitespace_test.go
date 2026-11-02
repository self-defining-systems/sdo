package validate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeAuthoredWhitespaceTouchesOnlyAuthoredMemory(t *testing.T) {
	root := t.TempDir()
	dirty := "line \t\r\nnext  \n\n\n"
	// Each file maps to its expected bytes after normalization.
	files := map[string]string{
		".sdo/playbooks/p/README.md":       "line\nnext\n",
		".sdo/arch.md":                     "line\nnext\n",
		".sdo/corpus/case/recordings.json": dirty,
		".sdo/rendered/" + sha64 + ".yaml": dirty,
		".sdo/outcomes/" + sha64 + ".json": dirty,
		"kubernetes/geo/configmap.yaml":    dirty,
	}
	paths := []string{".sdo/playbooks/deleted/README.md", ".sdo/playbooks/p/scripts/tabbed.sh"}
	for path := range files {
		write(t, root, path, dirty)
		paths = append(paths, path)
	}
	// Space before a tab has no single intended rewrite; it stays a diagnostic.
	files[".sdo/playbooks/p/scripts/tabbed.sh"] = " \tindented\n"
	write(t, root, ".sdo/playbooks/p/scripts/tabbed.sh", " \tindented\n")
	changed, err := NormalizeAuthoredWhitespace(root, paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed) != 2 {
		t.Fatalf("changed = %v, want the two authored files", changed)
	}
	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
}

const sha64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
