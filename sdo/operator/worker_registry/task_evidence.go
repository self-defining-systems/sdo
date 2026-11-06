package worker_registry

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// TaskEvidenceRoot is the repository-relative directory holding one subdirectory per agent task that has no
// incident session: lifecycle tasks (health judge, deployer). The runtime-artifacts export reads it.
const TaskEvidenceRoot = ".sdo-runtime/tasks"

const (
	taskEvidenceSchema  = "sdo.task-evidence/v1"
	taskEvidenceMaxText = 4096
)

var unsafeTaskKeyRune = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// TaskEvidenceKey names one task's evidence directory: a readable slug of the task ID plus a digest, so IDs that
// differ only in unsafe characters never collide. The directory's task.json carries the exact task ID.
func TaskEvidenceKey(taskID string) string {
	slug := strings.Trim(unsafeTaskKeyRune.ReplaceAllString(taskID, "-"), "-.")
	if len(slug) > 48 {
		slug = slug[:48]
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(taskID)))[:12]
	if slug == "" {
		return digest
	}
	return slug + "-" + digest
}

type taskEvidence struct {
	relative  string // repository-relative directory
	directory string // directory on the controller's repository mount
}

// Execute runs one agent task. A task without an incident session (a lifecycle task) is given a private,
// durable session directory keyed by its task ID: the agent CLI's rollout JSONL, usage and resolution logs land
// there through the worker's session mount, and the controller records the task request and its result or error.
// The pod's own filesystem vanishes when the Job completes; this directory survives, failed and retried tasks
// included, so every attempt of a task can be audited after the fact.
func (e KubernetesExecutor) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if e.SessionPVCSubPath != "" || task.GetTaskId() == "" || e.Client == nil || e.Registry == nil {
		return e.execute(ctx, task)
	}
	if e.RepositoryMountPath == "" {
		e.RepositoryMountPath = "/workspace"
	}
	evidence, err := e.prepareTaskEvidence(task)
	if err != nil {
		return nil, err
	}
	e.SessionPVCSubPath = path.Join(e.RepositoryPVCSubPath, evidence.relative)
	e.Environment = cloneEnvironment(e.Environment)
	e.Environment["SDO_TURN_USAGE_LOG"] = "/sdo/session/setup-usage.jsonl"
	e.Environment["CODEX_HOME"] = "/sdo/session/codex"
	e.Environment["CLAUDE_CONFIG_DIR"] = "/sdo/session/claude"
	result, err := e.execute(ctx, task)
	evidence.finish(result, err)
	return result, err
}

func (e KubernetesExecutor) prepareTaskEvidence(task *contracts.AgentTask) (taskEvidence, error) {
	relative := path.Join(TaskEvidenceRoot, TaskEvidenceKey(task.GetTaskId()))
	directory := filepath.Join(e.RepositoryMountPath, filepath.FromSlash(relative))
	if err := os.MkdirAll(directory, 0700); err != nil {
		return taskEvidence{}, fmt.Errorf("prepare task evidence for %q: %w", task.GetTaskId(), err)
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return taskEvidence{}, fmt.Errorf("encode task evidence request: %w", err)
	}
	meta, err := json.Marshal(map[string]string{"schema": taskEvidenceSchema, "task_id": task.GetTaskId(), "agent_id": task.GetAgentId(), "task_type": task.GetTaskType()})
	if err != nil {
		return taskEvidence{}, err
	}
	for name, data := range map[string][]byte{"task.json": meta, "request.json": payload} {
		if err := os.WriteFile(filepath.Join(directory, name), append(data, '\n'), 0600); err != nil {
			return taskEvidence{}, fmt.Errorf("record task evidence for %q: %w", task.GetTaskId(), err)
		}
	}
	return taskEvidence{relative: relative, directory: directory}, nil
}

// finish records the outcome and drops the provider login the worker copied into its session home. Recording is
// best effort: a full or unwritable volume must not turn a finished task into a failed one.
func (t taskEvidence) finish(result *contracts.AgentResult, err error) {
	for _, home := range []string{"codex/auth.json", "claude/.credentials.json"} {
		_ = os.Remove(filepath.Join(t.directory, filepath.FromSlash(home)))
	}
	switch {
	case result != nil:
		if payload, encodeErr := wire.Canonical(result); encodeErr == nil {
			_ = os.WriteFile(filepath.Join(t.directory, "result.json"), append(payload, '\n'), 0600)
		}
	case err != nil:
		text := err.Error()
		if len(text) > taskEvidenceMaxText {
			text = strings.ToValidUTF8(text[:taskEvidenceMaxText], "?")
		}
		if payload, encodeErr := json.Marshal(map[string]string{"schema": taskEvidenceSchema, "error": text}); encodeErr == nil {
			_ = os.WriteFile(filepath.Join(t.directory, "error.json"), append(payload, '\n'), 0600)
		}
	}
}
