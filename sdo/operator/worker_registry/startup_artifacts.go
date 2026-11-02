package worker_registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type WorkerStartup struct {
	WorkerReady           *time.Time `json:"worker_ready"`
	AgentProcessStarted   *time.Time `json:"agent_process_started"`
	CodexProcessStarted   *time.Time `json:"codex_process_started"`
	ClaudeProcessStarted  *time.Time `json:"claude_process_started"`
	FirstModelRequestSent *time.Time `json:"first_model_request_sent"`
	Source                string     `json:"first_model_request_source"`
}
type DaemonStartup struct {
	Imported *time.Time `json:"worker_imported_at"`
	Started  *time.Time `json:"registered_agent_process_started"`
	Released *time.Time `json:"worker_claim_released"`
}
type SessionStartup struct {
	ReceivedAt time.Time `json:"session_open_received_at"`
	OpenedAt   time.Time `json:"session_open_at"`
}
type DispatchStartup struct {
	Mode        string     `json:"mode"`
	ClaimedAt   *time.Time `json:"claimed_at"`
	TaskBoundAt time.Time  `json:"task_bound_at"`
}

// ValidateStartupArtifact keeps the export edge restricted to named timing
// DTOs. Unknown fields and non-time values cannot become an export channel.
func ValidateStartupArtifact(name string, data []byte) error {
	if len(data) > 64<<10 {
		return errors.New("startup timing artifact exceeds 64 KiB")
	}
	var value any
	switch name {
	case "startup-controller.json":
		value = new(ResponderStartup)
	case "startup-worker.json":
		value = new(WorkerStartup)
	case "startup-daemon.json":
		value = new(DaemonStartup)
	case "startup-session.json":
		value = new(SessionStartup)
	case "startup-dispatch.json":
		value = new(DispatchStartup)
	default:
		return fmt.Errorf("unknown startup timing artifact %q", name)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode startup timing %s: %w", name, err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("startup timing artifact has trailing data")
	}
	source := ""
	switch typed := value.(type) {
	case *WorkerStartup:
		source = typed.Source
	case *ResponderStartup:
		source = typed.FirstModelRequestSource
	}
	if source != "" && source != "codex.otel.api_request.timestamp-minus-duration" && source != "codex.otel.websocket_request.timestamp-minus-duration" {
		return errors.New("unknown first-model-request timing source")
	}
	return nil
}

// RecordSessionStartup is written by the controller tool handler, never the
// responder. The first successful session-open retains both arrival/completion.
func RecordSessionStartup(repositoryMount, pvcSubPath, sessionSubPath string, received, opened time.Time) error {
	relative, err := filepath.Rel(sessionPVCBase(pvcSubPath), sessionSubPath)
	if err != nil {
		return err
	}
	directory := filepath.Join(repositoryMount, relative, "startup")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	attempts := filepath.Join(directory, "session-opens")
	if err := os.MkdirAll(attempts, 0700); err != nil {
		return err
	}
	if err := writeStartupOnce(filepath.Join(attempts, fmt.Sprintf("%d.json", received.UnixNano())), SessionStartup{ReceivedAt: received, OpenedAt: opened}); err != nil {
		return err
	}
	filename := filepath.Join(directory, "startup-session.json")
	return writeStartupOnce(filename, SessionStartup{ReceivedAt: received, OpenedAt: opened})
}

func writeStartupOnce(filename string, value any) error {
	if _, err := os.Stat(filename); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(filename), ".startup-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err := file.Chmod(0600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// Publication is create-once and atomic: a rejoining controller must never
	// observe an empty or partially written original boundary.
	if err := os.Link(file.Name(), filename); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}
