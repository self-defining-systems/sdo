package worker_registry

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/safefs"
)

// The worker pod can write the task directory, so a link it plants must never redirect the controller's writes.
func TestTaskEvidenceNeverFollowsAgentPlantedLinks(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(outside, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	evidence := taskEvidence{root: root, relative: TaskEvidenceRoot + "/t1"}
	if err := safefs.MkdirAll(root, evidence.relative, 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, evidence.relative)
	if err := os.Symlink(outside, filepath.Join(dir, "error.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Join(dir, "codex")); err != nil {
		t.Fatal(err)
	}
	evidence.finish(nil, errors.New("boom"))
	if data, _ := os.ReadFile(outside); string(data) != "keep" {
		t.Fatalf("controller wrote through a planted link: %q", data)
	}
	// a link below the evidence directory is refused, not followed
	if err := safefs.Remove(root, evidence.relative+"/codex/victim"); !errors.Is(err, safefs.ErrSymlink) {
		t.Fatalf("want symlink refusal, got %v", err)
	}
}
