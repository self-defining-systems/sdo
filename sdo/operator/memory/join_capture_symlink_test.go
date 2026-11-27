package memory

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/safefs"
)

// The responder's pod mounts its session directory writable, so the capture
// directory below it can be a link the agent planted.
func TestCaptureJoinContextRefusesAPlantedCorpusLink(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	corpus, err := IncidentCorpusDir(root, "synthetic-incident")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, corpus); err != nil {
		t.Fatal(err)
	}
	err = CaptureJoinContext(root, "synthetic-incident", 1, JoinCaptureContext{})
	if !errors.Is(err, safefs.ErrSymlink) {
		t.Fatalf("want ErrSymlink, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, joinCaptureContextFile(1))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrote through the planted link: %v", err)
	}
}
