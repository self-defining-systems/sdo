package validate

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/safefs"
)

func TestNormalizersNeverWriteThroughALinkedParent(t *testing.T) {
	base := t.TempDir()
	checkout, outside := filepath.Join(base, "checkout"), filepath.Join(base, "outside")
	os.MkdirAll(filepath.Join(checkout), 0o755)
	os.MkdirAll(outside, 0o755)
	victim := filepath.Join(outside, "goal.md")
	os.WriteFile(victim, []byte("# Goal   \n"), 0o644)
	os.Symlink(outside, filepath.Join(checkout, ".sdo"))
	_, err := NormalizeAuthoredWhitespace(checkout, []string{".sdo/goal.md"})
	if err != nil && !errors.Is(err, safefs.ErrSymlink) {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(victim); string(data) != "# Goal   \n" {
		t.Fatalf("victim rewritten through .sdo link: %q", data)
	}
}

func TestCorpusNormalizerNeverRemovesThroughALinkedParent(t *testing.T) {
	base := t.TempDir()
	checkout, outside := filepath.Join(base, "checkout"), filepath.Join(base, "outside")
	os.MkdirAll(filepath.Join(checkout, filepath.FromSlash(layout.CorpusDir)), 0o755)
	os.MkdirAll(outside, 0o755)
	os.WriteFile(filepath.Join(outside, "fixed.json"), []byte("{}"), 0o644)
	os.WriteFile(filepath.Join(outside, "open.json"), []byte("{}"), 0o644)
	os.Symlink(outside, filepath.Join(checkout, filepath.FromSlash(layout.CorpusDir), "case1"))
	if _, err := NormalizeCapturedCorpus(checkout, []string{layout.CorpusDir + "/case1/open.json"}); err != nil {
		t.Logf("refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "open.json")); err != nil {
		t.Fatalf("file outside the checkout removed: %v", err)
	}
}
