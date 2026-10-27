package lifecycleio

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// TaskStore freezes the full registered request before any worker executes.
// It lives outside proposal checkouts, so resets cannot discard request bytes.
type TaskStore struct{ Root string }

func (s TaskStore) taskPath(id string) string {
	return filepath.Join(s.Root, fmt.Sprintf("%x.json", sha256.Sum256([]byte(id))))
}

func (s TaskStore) Load(id string) (*contracts.AgentTask, bool, error) {
	if s.Root == "" {
		return nil, false, fmt.Errorf("reflection task store root is required")
	}
	payload, err := os.ReadFile(s.taskPath(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	task := &contracts.AgentTask{}
	if err := wire.UnmarshalDurable(payload, task); err != nil {
		return nil, false, fmt.Errorf("read frozen reflection task: %w", err)
	}
	if task.GetTaskId() != id {
		return nil, false, fmt.Errorf("frozen reflection task identity mismatch")
	}
	return task, true, nil
}

// Discard drops a frozen task that can never run, so the next attempt freezes a new one.
func (s TaskStore) Discard(id string) error {
	if s.Root == "" {
		return fmt.Errorf("reflection task store root is required")
	}
	if err := os.Remove(s.taskPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s TaskStore) Save(task *contracts.AgentTask) error {
	payload, err := wire.Canonical(task)
	if err != nil {
		return err
	}
	if s.Root == "" {
		return fmt.Errorf("reflection task store root is required")
	}
	if err := os.MkdirAll(s.Root, 0700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.Root, ".task-")
	if err != nil {
		return err
	}
	defer os.Remove(temp.Name())
	if _, err := temp.Write(payload); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	// Link is an atomic create-if-absent: a crash never exposes a partial task,
	// and a competing writer cannot replace the first request with new bytes.
	if err := os.Link(temp.Name(), s.taskPath(task.GetTaskId())); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := os.ReadFile(s.taskPath(task.GetTaskId()))
		if readErr != nil {
			return readErr
		}
		if string(existing) != string(payload) {
			return fmt.Errorf("frozen reflection task has different payload")
		}
	}
	dir, err := os.Open(s.Root)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
