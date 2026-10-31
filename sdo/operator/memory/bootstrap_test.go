package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
)

func TestWriteSchemaRejectsOldRepositoryWithoutChangingIt(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			root := t.TempDir()
			directory := filepath.Join(root, layout.Dir)
			if err := os.MkdirAll(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, layout.SchemaVersionFile)
			original := fmt.Sprintf("%d\n", version)
			if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
				t.Fatal(err)
			}
			err := WriteSchema(root)
			want := fmt.Sprintf("unsupported operational memory schema version %d; required %d", version, layout.CurrentSchemaVersion)
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v; want %s", err, want)
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != original {
				t.Fatalf("schema changed: %q, %v", data, err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 1 {
				t.Fatalf("bootstrap edited old repository: %v, %v", entries, err)
			}
		})
	}
}
