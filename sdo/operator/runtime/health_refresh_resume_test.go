package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

func tracingMemoryAdvance(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, ".sdo/playbooks/base/README.md")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(content, []byte("\nMemory-only guidance.\n")...), 0644); err != nil {
		t.Fatal(err)
	}
}
