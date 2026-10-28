package memory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/memory/layout"
	"strings"
	"sync"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var capturedRecordingsMu sync.Mutex

func AppendCapturedRecording(repositoryMount, incidentID, phase string, call sdktest.PrimitiveCall) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

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
	if call.Method == "RepairReceipt" {
		for _, prior := range calls {
			if prior.Method == call.Method && string(prior.Result) == string(call.Result) {
				return nil
			}
		}
	}
	if call.Method == "DatabaseUsers" {
		identity, err := databaseCaptureIdentity(call)
		if err != nil {
			return err
		}
		for _, prior := range calls {
			if prior.Method != "DatabaseUsers" {
				continue
			}
			previous, err := databaseCaptureIdentity(prior)
			if err != nil {
				return err
			}
			// Catalogs are mutable. A later responder call cannot replace the
			// first observation at this immutable capture boundary, even when
			// it selects another credential source or an equivalent shell.
			if identity == previous {
				return nil
			}
		}
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

func databaseCaptureIdentity(call sdktest.PrimitiveCall) (string, error) {
	var targets []program.DatabaseTarget
	if err := json.Unmarshal(call.Arguments, &targets); err != nil || len(targets) != 1 {
		return "", fmt.Errorf("invalid captured database target")
	}
	target := targets[0]
	if err := target.Validate(); err != nil {
		return "", fmt.Errorf("invalid captured database target")
	}
	target.Auth = nil
	target.Shell = ""
	phase := call.Phase
	if phase == "" {
		phase = "open"
	}
	raw, _ := json.Marshal(struct {
		Phase  string
		Target program.DatabaseTarget
	}{phase, target})
	return string(raw), nil
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
	return ValidateCapturedCorpusDirectory(trusted, candidate, incidentID, changedPaths)
}

// ValidateCapturedCorpusDirectory checks a read-only session mount with the same
// provenance rules used by the controller's repository-backed capture store.
func ValidateCapturedCorpusDirectory(trusted, candidate, incidentID string, changedPaths []string) error {
	root := filepath.Join(candidate, layout.CorpusDir)
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
			if strings.HasPrefix(path, layout.CorpusDir+"/"+entry.Name()+"/") {
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
			RootCauseProgramIDs    []string                 `json:"root_cause_program_ids"`
			NoMatchProgramIDs      []string                 `json:"no_match_program_ids"`
			SyntheticNoMatchReason string                   `json:"synthetic_no_match_reason"`
			Provenance             sdktest.CorpusProvenance `json:"provenance"`
		}
		if err := json.Unmarshal(labels, &label); err != nil {
			return err
		}
		if label.Provenance.IncidentID == "" && label.SyntheticNoMatchReason != "" && len(label.RootCauseProgramIDs) == 0 && len(label.NoMatchProgramIDs) > 0 {
			continue
		}
		if strings.TrimSpace(incidentID) == "" {
			return fmt.Errorf("corpus %s has no provisioned captured incident identity", entry.Name())
		}
		if label.Provenance.IncidentID == "" {
			return fmt.Errorf("corpus %s lacks captured incident provenance", entry.Name())
		}
		if label.Provenance.IncidentID != incidentID {
			return fmt.Errorf("corpus %s references another captured incident", entry.Name())
		}
		openName := "open.json"
		if label.Provenance.OpenAt == sdktest.CapturedFixedNoMatchPhase && len(label.RootCauseProgramIDs) == 0 && len(label.NoMatchProgramIDs) > 0 {
			openName = "fixed.json"
		} else if label.Provenance.OpenAt != "" {
			if label.Provenance.OpenAt != "detect" && label.Provenance.OpenAt != "settle" && label.Provenance.OpenAt != "pre-apply" {
				return fmt.Errorf("invalid captured open phase %q", label.Provenance.OpenAt)
			}
			openName = "open@" + label.Provenance.OpenAt + ".json"
		} else {
			for _, phase := range []string{"open@pre-apply", "open@settle", "open@detect"} {
				data, readErr := os.ReadFile(filepath.Join(trusted, phase+".json"))
				if readErr == nil && sdktest.SnapshotMatchesDigest(data, label.Provenance.OpenSHA256) {
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
				if openName == "fixed.json" {
					trustedName = "open@detect-recordings.json"
				}
			}
			if name == "recordings.json" && openName == "fixed.json" {
				got, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil {
					return err
				}
				exact := false
				for _, phase := range []string{"detect", "pre-apply"} {
					want, err := os.ReadFile(filepath.Join(trusted, "open@"+phase+"-recordings.json"))
					exact = exact || (err == nil && string(got) == string(want))
				}
				if !exact {
					return fmt.Errorf("corpus %s recordings differ from captured incident observations", entry.Name())
				}
				continue
			}
			want, err := os.ReadFile(filepath.Join(trusted, trustedName))
			if err != nil {
				return fmt.Errorf("read trusted captured %s: %w", name, err)
			}
			got, err := os.ReadFile(sdktest.CorpusSnapshotFile(dir, name))
			if err != nil {
				return err
			}
			same := string(got) == string(want)
			if name != "recordings.json" {
				// Snapshots compare by content: an earlier capture layout and the
				// objects-only form of the same snapshot are equal.
				gotDigest, gotErr := sdktest.SnapshotDigest(got)
				wantDigest, wantErr := sdktest.SnapshotDigest(want)
				same = gotErr == nil && wantErr == nil && gotDigest == wantDigest
			}
			if !same {
				return fmt.Errorf("corpus %s %s differs from captured incident snapshot", entry.Name(), strings.TrimSuffix(name, ".json"))
			}
		}
	}
	return nil
}

// CaptureIncidentSnapshot stores the first namespace-local view at each boundary.
func CaptureIncidentSnapshot(repositoryMount, incidentID, phase string, view sdk.DetectionContext, namespace string, refs ...sdk.ObjectRef) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

	if phase != "open" && phase != "fixed" && phase != "open@detect" && phase != "open@settle" && phase != "open@pre-apply" {
		return fmt.Errorf("invalid corpus capture phase %q", phase)
	}
	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	refs, err = CapturedSessionRefs(filepath.Dir(dir), refs)
	if err != nil {
		return err
	}
	snapshot, err := sdktest.CaptureSnapshot(view, namespace, refs...)
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshot)
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

// CaptureRepairedProgramReads records fresh answers at the controller's verified
// fixed boundary for every primitive inspected before repair. They travel in
// the same protected recordings file with explicit phase metadata.
func CaptureRepairedProgramReads(repositoryMount, incidentID string, readView func(sdk.DetectionContext) program.ReadView) error {
	releaseSessionStorage := SessionStorageUse()
	defer releaseSessionStorage()

	dir, err := IncidentCorpusDir(repositoryMount, incidentID)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "fixed.json"))
	if err != nil {
		return err
	}
	var snapshot sdktest.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return err
	}
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		raw, err := os.ReadFile(filepath.Join(dir, phase+"-recordings.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			return err
		}
		var open []sdktest.PrimitiveCall
		fixed := false
		for _, call := range calls {
			if call.Phase == "fixed" {
				fixed = true
			}
			if call.Phase == "" || call.Phase == "open" {
				open = append(open, call)
			}
		}
		if fixed || len(open) == 0 {
			continue
		}
		observed, err := sdktest.RecordPrimitiveCalls(readView(snapshot), open)
		if err != nil {
			return err
		}
		for _, call := range observed {
			call.Phase = "fixed"
			if err := AppendCapturedRecording(repositoryMount, incidentID, phase, call); err != nil {
				return err
			}
		}
	}
	return nil
}
