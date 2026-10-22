package worker_registry

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	preparedTaskFile = "prepared-task.json"
	// PreparedTaskReferencePrefix starts the reference a state document holds in place of staged task bytes.
	PreparedTaskReferencePrefix = "staged-task:sha256:"
)

// PreparedTaskUnavailableError reports that the bytes a state document references are missing or differ from
// the digest the controller recorded.
type PreparedTaskUnavailableError struct {
	JobName   string
	Reference string
	Err       error
}

func (err *PreparedTaskUnavailableError) Error() string {
	return fmt.Sprintf("prepared agent task for job %q (%s) is unavailable on the repository volume: %v", err.JobName, err.Reference, err.Err)
}
func (err *PreparedTaskUnavailableError) Unwrap() error { return err.Err }

// PreparedTaskStore keeps the controller's frozen canonical task beside the Job's staged request, so the state
// ConfigMap holds only a digest reference. The file lives under the same per-job directory as a staged request.
type PreparedTaskStore struct{ MountPath string }

// IsPreparedTaskReference reports whether value is a reference rather than task bytes.
func IsPreparedTaskReference(value string) bool {
	return strings.HasPrefix(value, PreparedTaskReferencePrefix)
}

// Stage writes payload before any state document references it and returns the reference. A retry whose bytes
// already match the staged file reuses it.
func (s PreparedTaskStore) Stage(jobName string, payload []byte) (string, error) {
	sum := sha256.Sum256(payload)
	reference := PreparedTaskReferencePrefix + hex.EncodeToString(sum[:])
	if existing, err := os.ReadFile(s.path(jobName)); err == nil && sha256.Sum256(existing) == sum {
		return reference, nil
	}
	if err := writeStaged(s.MountPath, jobName, preparedTaskFile, payload); err != nil {
		return "", &PreparedTaskUnavailableError{JobName: jobName, Reference: reference, Err: err}
	}
	return reference, nil
}

// Resolve returns the staged bytes after verifying them against the reference digest.
func (s PreparedTaskStore) Resolve(jobName, reference string) ([]byte, error) {
	payload, err := os.ReadFile(s.path(jobName))
	if err != nil {
		return nil, &PreparedTaskUnavailableError{JobName: jobName, Reference: reference, Err: err}
	}
	sum := sha256.Sum256(payload)
	if PreparedTaskReferencePrefix+hex.EncodeToString(sum[:]) != reference {
		return nil, &PreparedTaskUnavailableError{JobName: jobName, Reference: reference, Err: errors.New("staged bytes do not match the recorded digest")}
	}
	return payload, nil
}

// Release removes the staged task once no state document needs it.
func (s PreparedTaskStore) Release(jobName string) {
	_ = os.Remove(s.path(jobName))
	_ = os.Remove(filepath.Dir(s.path(jobName)))
}

func (s PreparedTaskStore) path(jobName string) string {
	return filepath.Join(s.MountPath, filepath.FromSlash(stagedRequestsDir), jobName, preparedTaskFile)
}

// writeStaged atomically writes name under the Job's directory of the repository volume.
func writeStaged(mountPath, jobName, name string, payload []byte) error {
	directory := filepath.Join(mountPath, filepath.FromSlash(stagedRequestsDir), jobName)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".staged-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	_, err = temporary.Write(payload)
	if err == nil {
		err = temporary.Chmod(0o644)
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(temporary.Name(), filepath.Join(directory, name))
}
