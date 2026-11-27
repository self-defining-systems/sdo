package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/safefs"
)

// The routing note and sweep marker live in the capture directory, below a
// session directory the agent can write.
func TestRoutingCaptureRefusesPlantedLinks(t *testing.T) {
	for _, name := range []string{"routing-probe-note.json", "open@detect-routing.done"} {
		t.Run(name, func(t *testing.T) {
			root, outside := t.TempDir(), filepath.Join(t.TempDir(), "outside")
			dir, err := memory.IncidentCorpusDir(root, "synthetic-incident")
			if err != nil {
				t.Fatal(err)
			}
			for file, content := range map[string]string{"open@detect.json": `{}`, "open@detect-recordings.json": `[]`} {
				if err := memory.CreateCaptureFile(root, filepath.Join(dir, file), []byte(content)); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(outside, filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
			if name == "routing-probe-note.json" {
				err = recordRoutingProbeUnavailable(root, dir, "synthetic-incident", "open@detect", "probe_failed", "")
			} else {
				err = captureRoutingEvidence(context.Background(), root, "synthetic-incident", "open@detect", nil, nil)
			}
			if !errors.Is(err, safefs.ErrSymlink) {
				t.Fatalf("want ErrSymlink, got %v", err)
			}
			if _, err := os.Lstat(outside); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("wrote through the planted link: %v", err)
			}
		})
	}
}
