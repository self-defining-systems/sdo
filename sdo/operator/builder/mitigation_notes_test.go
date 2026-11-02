package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Replay notes are informational receipts read back from the workspace; an
// oversized file is refused rather than loaded into memory.
func TestReplayNotesOverOneMiBAreRefused(t *testing.T) {
	dir := t.TempDir()
	generated := filepath.Join(dir, "generated")
	if err := os.MkdirAll(generated, 0o755); err != nil {
		t.Fatal(err)
	}
	workspace := &Workspace{Directory: dir}
	pad := func(n int) []byte { return []byte("[" + strings.Repeat(" ", n-2) + "]") }
	for name, read := range map[string]func() error{
		"mitigation-replay.json": func() error { _, err := workspace.MitigationReplayResults(); return err },
		"notes.json":             func() error { _, err := workspace.generatedNotes("notes.json"); return err },
	} {
		if err := os.WriteFile(filepath.Join(generated, name), pad(1<<20), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := read(); err != nil {
			t.Errorf("%s at exactly 1 MiB: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(generated, name), pad(1<<20+1), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := read(); err == nil || !strings.Contains(err.Error(), "exceed 1 MiB") {
			t.Errorf("%s over 1 MiB: want a size refusal, got %v", name, err)
		}
	}
}
