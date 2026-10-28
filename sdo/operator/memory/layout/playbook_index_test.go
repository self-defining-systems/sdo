package layout

import (
	"os"
	"path/filepath"
	"testing"
)

func writeIndexFixture(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeneratePlaybookIndexListsActivePlaybooksOnceByID(t *testing.T) {
	root := t.TempDir()
	writeIndexFixture(t, root, map[string]string{
		PlaybookIndexFile:                      "# Playbooks\n\n- [beta](beta/README.md)\n- [Beta fix](beta/README.md)\n- [alpha](alpha/README.md)\n",
		".sdo/playbooks/beta/README.md":        "---\nowner: responder\n---\n# Beta [fix]\n\nBody\n",
		".sdo/playbooks/alpha/README.md":       "---\nowner: responder\n---\nNo heading here\n",
		".sdo/playbooks/retired-one/README.md": "---\nstatus: retired\nretired_reason: superseded\n---\n# Old\n",
		".sdo/playbooks/no-readme/diagnose.go": "package playbook\n",
		".sdo/playbooks/plain/README.md":       "# Plain title\n",
		".sdo/playbooks/Not_Valid/README.md":   "# Ignored\n",
	})
	got, err := GeneratePlaybookIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	want := "# Playbooks\n\n- [alpha](alpha/README.md)\n- [Beta \\[fix\\]](beta/README.md)\n- [Plain title](plain/README.md)\n"
	if string(got) != want {
		t.Fatalf("index =\n%s\nwant\n%s", got, want)
	}
	changed, err := WritePlaybookIndex(root)
	if err != nil || !changed {
		t.Fatalf("WritePlaybookIndex changed=%v err=%v", changed, err)
	}
	if data, _ := os.ReadFile(filepath.Join(root, PlaybookIndexFile)); string(data) != want {
		t.Fatalf("written index = %q", data)
	}
	if changed, err := WritePlaybookIndex(root); err != nil || changed {
		t.Fatalf("second write changed=%v err=%v, want idempotent", changed, err)
	}
}

func TestWritePlaybookIndexLeavesMemoryWithoutPlaybooksAlone(t *testing.T) {
	root := t.TempDir()
	if changed, err := WritePlaybookIndex(root); err != nil || changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if _, err := os.Stat(filepath.Join(root, PlaybookIndexFile)); !os.IsNotExist(err) {
		t.Fatalf("index created without playbooks: %v", err)
	}
}
