package layout

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOfflineMigrationCurrentAndFutureSchema(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, Dir), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, SchemaVersionFile), []byte("3\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if changed, err := Migrate(root); err != nil || changed {
		t.Fatalf("current changed=%v: %v", changed, err)
	}
	if err := os.WriteFile(filepath.Join(root, SchemaVersionFile), []byte("999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(root); err == nil {
		t.Fatal("future schema accepted")
	}
}
