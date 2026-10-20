package memory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// IncidentCorpusDir locates trusted controller captures outside accepted memory.
func IncidentCorpusDir(repositoryMount, incidentID string) (string, error) {
	if _, err := PrepareIncidentSession(repositoryMount, "", incidentID); err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(incidentID)))[:32]
	return filepath.Join(repositoryMount, ".sdo-runtime", "sessions", digest, "corpus"), nil
}

// ValidateCapturedCorpusTree compares proposed positive cases with the
// controller's incident session before the broker publishes them.
func ValidateCapturedCorpusTree(repositoryMount, candidate, incidentID string, changedPaths []string) error {
	trusted, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	root := filepath.Join(candidate, ".sdo", "corpus")
	entries, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		changed := false
		for _, path := range changedPaths {
			if strings.HasPrefix(path, ".sdo/corpus/"+entry.Name()+"/") {
				changed = true
				break
			}
		}
		if !changed {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		labels, err := os.ReadFile(filepath.Join(dir, "labels.json"))
		if err != nil {
			return err
		}
		var label struct {
			Provenance struct {
				IncidentID string `json:"incident_id"`
			} `json:"provenance"`
		}
		if err := json.Unmarshal(labels, &label); err != nil {
			return err
		}
		if label.Provenance.IncidentID == "" {
			continue
		}
		if label.Provenance.IncidentID != incidentID {
			return fmt.Errorf("corpus %s references another captured incident", entry.Name())
		}
		for _, name := range []string{"open.json", "fixed.json", "recordings.json"} {
			want, err := os.ReadFile(filepath.Join(trusted, name))
			if err != nil {
				return fmt.Errorf("read trusted captured %s: %w", name, err)
			}
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return err
			}
			if string(got) != string(want) {
				return fmt.Errorf("corpus %s %s differs from captured incident snapshot", entry.Name(), strings.TrimSuffix(name, ".json"))
			}
		}
	}
	return nil
}

// CaptureIncidentSnapshot stores the first namespace-local view at each boundary.
func CaptureIncidentSnapshot(repositoryMount, incidentID, phase string, view sdk.DetectionContext, namespace string) error {
	if phase != "open" && phase != "fixed" {
		return fmt.Errorf("invalid corpus capture phase %q", phase)
	}
	snapshot, err := sdktest.CaptureSnapshot(view, namespace)
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if phase == "open" {
		if err := writeCapturedFile(filepath.Join(dir, "recordings.json"), []byte("[]")); err != nil {
			return err
		}
	}
	return writeCapturedFile(filepath.Join(dir, phase+".json"), data)
}

func writeCapturedFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".capture-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Chmod(0444); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}
