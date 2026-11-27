package memory

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"sdo.dev/operator/safefs"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func healthyBaselinePath(root, revision string) string {
	return filepath.Join(root, ".sdo-runtime", "healthy", fmt.Sprintf("%x.json", sha256.Sum256([]byte(revision))))
}

// CaptureHealthyBaseline is called only after a clear controller evaluation.
// Captures live outside accepted memory and are never writable by workers.
// The reference identity binds accepted source and the observation boundary.
func CaptureHealthyBaseline(root, revision string, view sdk.DetectionContext, namespace string) error {
	snapshot, err := sdktest.CaptureSnapshot(view, namespace)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	rel, err := safefs.Rel(root, healthyBaselinePath(root, revision))
	if err != nil {
		return err
	}
	if err := safefs.MkdirAll(root, path.Dir(rel), 0700); err != nil {
		return err
	}
	return safefs.WriteSyncedFile(root, rel, raw, 0444)
}

// BindIncidentHealthyBaseline freezes the pre-incident reference. A later
// successful repair must never replace the healthy reference for this incident.
func BindIncidentHealthyBaseline(root, revision, incidentID string) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

	raw, err := os.ReadFile(healthyBaselinePath(root, revision))
	if err != nil {
		return err
	}
	dir, err := IncidentCorpusDir(root, incidentID)
	if err != nil {
		return err
	}
	dir = filepath.Join(dir, "healthy")
	if err := CreateCaptureFile(root, filepath.Join(dir, "healthy.json"), raw); err != nil {
		return err
	}
	return CreateCaptureFile(root, filepath.Join(dir, "reference"), []byte(revision+"\n"))
}

// RemoveHealthyBaseline prunes an obsolete candidate. Incident references are
// independent frozen files and remain intact after this private capture expires.
func RemoveHealthyBaseline(root, reference string) error {
	rel, err := safefs.Rel(root, healthyBaselinePath(root, reference))
	if err != nil {
		return err
	}
	return safefs.Remove(root, rel)
}

// BindClosureHealthyBaseline gives an incident without a pre-incident capture
// the controller's own post-closure healthy snapshot as its healthy sample. It
// is called only after the controller verified the closure healthy and captured
// "fixed"; an existing pre-incident reference is never replaced.
func BindClosureHealthyBaseline(root, incidentID string) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

	dir, err := IncidentCorpusDir(root, incidentID)
	if err != nil {
		return err
	}
	healthy := filepath.Join(dir, "healthy")
	if _, err := os.Stat(filepath.Join(healthy, "healthy.json")); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
	if err != nil {
		return err
	}
	if err := CreateCaptureFile(root, filepath.Join(healthy, "healthy.json"), raw); err != nil {
		return err
	}
	return CreateCaptureFile(root, filepath.Join(healthy, "reference"), []byte("closure\n"))
}
