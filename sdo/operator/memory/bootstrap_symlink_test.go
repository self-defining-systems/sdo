package memory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteArchitectureNeverWritesThroughACheckoutLink(t *testing.T) {
	base := t.TempDir()
	checkout, outside := filepath.Join(base, "checkout"), filepath.Join(base, "outside")
	os.MkdirAll(checkout, 0o755)
	os.MkdirAll(outside, 0o755)
	os.Symlink(outside, filepath.Join(checkout, ".sdo"))
	if err := WriteArchitecture(checkout, "app", "abc123", "summary"); err == nil {
		t.Fatal("WriteArchitecture followed a planted .sdo symlink")
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("files created outside the checkout: %v", entries)
	}
}
