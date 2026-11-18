package processio

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScopedTempHoldsWhatTheProcessLeavesAndTouchesNothingElse(t *testing.T) {
	base, goBase := t.TempDir(), t.TempDir()
	t.Setenv("TMPDIR", base)
	t.Setenv("GOTMPDIR", goBase)
	other := filepath.Join(goBase, "go-build-of-another-process")
	if err := os.Mkdir(other, 0o755); err != nil {
		t.Fatal(err)
	}
	cleanup, err := ScopedTemp("warm-")
	if err != nil {
		t.Fatal(err)
	}
	scoped := Environment("GOTMPDIR")
	if filepath.Dir(scoped) != goBase || filepath.Dir(Environment("TMPDIR")) != base {
		t.Fatalf("not scoped inside the originals: %s %s", Environment("TMPDIR"), scoped)
	}
	if err := os.Mkdir(filepath.Join(scoped, "go-build123"), 0o755); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if Environment("TMPDIR") != base || Environment("GOTMPDIR") != goBase {
		t.Fatalf("environment not restored: %s %s", Environment("TMPDIR"), Environment("GOTMPDIR"))
	}
	if entries, _ := os.ReadDir(base); len(entries) != 0 {
		t.Fatalf("left in TMPDIR: %v", entries)
	}
	entries, _ := os.ReadDir(goBase)
	if len(entries) != 1 || entries[0].Name() != "go-build-of-another-process" {
		t.Fatalf("GOTMPDIR must keep exactly what was not ours: %v", entries)
	}
}

func TestScopedTempWithoutGOTMPDIRScopesOnlyTMPDIR(t *testing.T) {
	base := t.TempDir()
	t.Setenv("TMPDIR", base)
	t.Setenv("GOTMPDIR", "")
	cleanup, err := ScopedTemp("warm-")
	if err != nil {
		t.Fatal(err)
	}
	if Environment("GOTMPDIR") != "" {
		t.Fatal("GOTMPDIR must stay unset")
	}
	cleanup()
	if entries, _ := os.ReadDir(base); len(entries) != 0 {
		t.Fatalf("left in TMPDIR: %v", entries)
	}
}
