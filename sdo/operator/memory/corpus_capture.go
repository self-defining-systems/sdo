package memory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

var capturedRecordingsMu sync.Mutex

func AppendCapturedRecording(repositoryMount, incidentID, phase string, call sdktest.PrimitiveCall) error {
	if phase != "open@detect" && phase != "open@settle" && phase != "open@pre-apply" {
		return fmt.Errorf("invalid corpus recording phase %q", phase)
	}
	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, phase+"-recordings.json")
	capturedRecordingsMu.Lock()
	defer capturedRecordingsMu.Unlock()
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(data, &calls); err != nil {
		return err
	}
	calls = append(calls, call)
	data, err = json.Marshal(calls)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".recordings-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

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
				OpenAt     string `json:"open_at"`
				OpenSHA256 string `json:"open_sha256"`
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
		openName := "open.json"
		if label.Provenance.OpenAt != "" {
			if label.Provenance.OpenAt != "detect" && label.Provenance.OpenAt != "settle" && label.Provenance.OpenAt != "pre-apply" {
				return fmt.Errorf("invalid captured open phase %q", label.Provenance.OpenAt)
			}
			openName = "open@" + label.Provenance.OpenAt + ".json"
		} else {
			for _, phase := range []string{"open@pre-apply", "open@settle", "open@detect"} {
				data, readErr := os.ReadFile(filepath.Join(trusted, phase+".json"))
				if readErr == nil && fmt.Sprintf("%x", sha256.Sum256(data)) == label.Provenance.OpenSHA256 {
					openName = phase + ".json"
					break
				}
			}
		}
		for _, name := range []string{"open.json", "fixed.json", "recordings.json"} {
			trustedName := name
			if name == "open.json" {
				trustedName = openName
			}
			if name == "recordings.json" && openName != "open.json" {
				trustedName = strings.TrimSuffix(openName, ".json") + "-recordings.json"
			}
			want, err := os.ReadFile(filepath.Join(trusted, trustedName))
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
	if phase != "open" && phase != "fixed" && phase != "open@detect" && phase != "open@settle" && phase != "open@pre-apply" {
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
	if phase == "open" || strings.HasPrefix(phase, "open@") {
		recordingName := "recordings.json"
		if phase != "open" {
			recordingName = phase + "-recordings.json"
		}
		if err := writeCapturedFile(filepath.Join(dir, recordingName), []byte("[]")); err != nil {
			return err
		}
	}
	return writeCapturedFile(filepath.Join(dir, phase+".json"), data)
}

// ReuseDriftCapture makes the investigated change the incident's detect-time
// corpus case. CaptureIncidentSnapshot will retain these first-written files.
func ReuseDriftCapture(repositoryMount, captureID, incidentID string) error {
	from, err := IncidentCorpusDir(repositoryMount, captureID)
	if err != nil {
		return err
	}
	to, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(to, 0700); err != nil {
		return err
	}
	for _, name := range []string{"open@detect.json", "open@detect-recordings.json"} {
		source, destination := filepath.Join(from, name), filepath.Join(to, name)
		if err := os.Link(source, destination); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("reuse investigated drift capture %s: %w", name, err)
			}
			before, readErr := os.ReadFile(source)
			if readErr != nil {
				return readErr
			}
			after, readErr := os.ReadFile(destination)
			if readErr != nil {
				return readErr
			}
			if string(before) != string(after) {
				return fmt.Errorf("incident %s has a different %s capture", incidentID, name)
			}
		}
	}
	return nil
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
