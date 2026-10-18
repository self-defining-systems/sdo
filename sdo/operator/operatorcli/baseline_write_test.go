package operatorcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBaselineWriteReplacesOnlyNamedSnapshots(t *testing.T) {
	root := filepath.Join(t.TempDir(), "healthy")
	if err := RunBaselineWrite(strings.NewReader(`{"healthy-0.json":{"namespace":"demo"}}`), root); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(root, "healthy-0.json")); err != nil || !strings.Contains(string(data), `"namespace":"demo"`) {
		t.Fatalf("snapshot not published: %s %v", data, err)
	}
	if err := RunBaselineWrite(strings.NewReader(`{"../escape.json":{}}`), root); err == nil {
		t.Fatal("unsafe snapshot name accepted")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape.json")); !os.IsNotExist(err) {
		t.Fatalf("unsafe snapshot escaped target: %v", err)
	}
}
