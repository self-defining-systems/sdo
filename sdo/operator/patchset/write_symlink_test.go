package patchset

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteWorkingNeverWritesThroughALinkedFile(t *testing.T) {
	base := t.TempDir()
	root, outside := filepath.Join(base, "root"), filepath.Join(base, "outside")
	os.MkdirAll(filepath.Join(root, "app"), 0o755)
	os.MkdirAll(outside, 0o755)
	victim := filepath.Join(outside, "victim")
	os.WriteFile(victim, []byte("precious"), 0o644)
	os.Symlink(victim, filepath.Join(root, "app/main.go"))
	err := writeWorking(root, []change{{path: "app/main.go", status: 'M', mode: "100644", content: []byte("patched")}})
	if err == nil {
		t.Fatal("writeWorking followed a planted symlink")
	}
	if data, _ := os.ReadFile(victim); string(data) != "precious" {
		t.Fatalf("victim overwritten: %q", data)
	}
}
