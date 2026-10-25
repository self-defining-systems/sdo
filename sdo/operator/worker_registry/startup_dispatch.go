package worker_registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Persist the boundary before releasing work. A rejoin uses this Job's original
// clock, while a continuation has a distinct immutable execution identity.
func (e KubernetesExecutor) prepareDispatchStartup(task *contracts.AgentTask, jobName string) (DispatchStartup, error) {
	timing := DispatchStartup{Mode: "cold", TaskBoundAt: time.Now().UTC()}
	if e.WarmBinding != nil {
		timing.Mode = "warm"
		timing.ClaimedAt = &e.WarmBinding.ClaimedAt
	}
	if e.SessionPVCSubPath == "" || !task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		return timing, nil
	}
	relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath)
	if err != nil {
		return timing, err
	}
	session := filepath.Join(e.RepositoryMountPath, relative)
	if _, err := os.Stat(session); errors.Is(err, os.ErrNotExist) {
		return timing, nil
	} else if err != nil {
		return timing, err
	}
	directory := filepath.Join(session, "startup/attempts", jobName)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return timing, err
	}
	filename := filepath.Join(directory, "startup-dispatch.json")
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		// Adopt an old initial-attempt record only when this is that same task.
		// The incident-wide file must never supply a continuation's boundary.
		incident := new(contracts.IncidentRequest)
		if task.Request.UnmarshalTo(incident) == nil && task.GetTaskId() == incident.GetIncidentId() {
			if data, err := os.ReadFile(filepath.Join(session, "startup/startup-dispatch.json")); err == nil {
				var previous DispatchStartup
				if json.Unmarshal(data, &previous) == nil && previous.Mode == timing.Mode && !previous.TaskBoundAt.IsZero() &&
					((previous.ClaimedAt == nil && timing.ClaimedAt == nil) || (previous.ClaimedAt != nil && timing.ClaimedAt != nil && previous.ClaimedAt.Equal(*timing.ClaimedAt))) {
					timing = previous
				}
			}
		}
	} else if err != nil {
		return timing, err
	}
	if err := writeStartupOnce(filename, timing); err != nil {
		return timing, err
	}
	data, err := os.ReadFile(filename)
	if err != nil {
		return timing, err
	}
	if err := json.Unmarshal(data, &timing); err != nil {
		return timing, err
	}
	if timing.TaskBoundAt.IsZero() {
		return timing, errors.New("missing persisted task-bound clock")
	}
	if err := writeStartupOnce(filepath.Join(session, "startup/startup-dispatch.json"), timing); err != nil {
		return timing, err
	}
	return timing, nil
}
