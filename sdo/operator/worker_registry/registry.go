// Package worker_registry resolves registered agent workers by their declared
// task and protobuf message types. It knows launch artifacts, not worker code.
package worker_registry

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const SchemaVersion = "sdo.dev/v1alpha1"

type Registry struct {
	byID map[string]*contracts.AgentRegistration
}

// New validates registrations and snapshots them so callers cannot alter
// permissions or launch commands after registration.
func New(registrations ...*contracts.AgentRegistration) (*Registry, error) {
	registry := &Registry{byID: make(map[string]*contracts.AgentRegistration, len(registrations))}
	for _, registration := range registrations {
		if err := validateRegistration(registration); err != nil {
			return nil, err
		}
		if _, exists := registry.byID[registration.GetAgentId()]; exists {
			return nil, fmt.Errorf("duplicate agent ID %q", registration.GetAgentId())
		}
		registry.byID[registration.GetAgentId()] = proto.Clone(registration).(*contracts.AgentRegistration)
	}
	return registry, nil
}

// LoadDirectory reads one strict ProtoJSON registration from each JSON file.
// The filename is administrative; agent identity comes from the message.
func LoadDirectory(directory string) (*Registry, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read agent registrations %q: %w", directory, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	registrations := make([]*contracts.AgentRegistration, 0, len(names))
	for _, name := range names {
		path := filepath.Join(directory, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read agent registration %q: %w", path, err)
		}
		registration := &contracts.AgentRegistration{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, registration); err != nil {
			return nil, fmt.Errorf("decode agent registration %q: %w", path, err)
		}
		registrations = append(registrations, registration)
	}
	return New(registrations...)
}

func validateRegistration(registration *contracts.AgentRegistration) error {
	if registration == nil {
		return fmt.Errorf("nil agent registration")
	}
	id := registration.GetAgentId()
	if registration.GetSchemaVersion() != SchemaVersion || strings.TrimSpace(id) == "" {
		return fmt.Errorf("agent registration requires schema version %q and nonempty agent ID", SchemaVersion)
	}
	if len(registration.GetCapabilities()) == 0 {
		return fmt.Errorf("agent %q declares no capabilities", id)
	}
	seen := make(map[string]bool)
	for _, capability := range registration.GetCapabilities() {
		if capability == nil || capability.GetTaskType() == "" || capability.GetRequestTypeUrl() == "" || capability.GetResultTypeUrl() == "" {
			return fmt.Errorf("agent %q has incomplete capability", id)
		}
		if seen[capability.GetTaskType()] {
			return fmt.Errorf("agent %q repeats task type %q", id, capability.GetTaskType())
		}
		seen[capability.GetTaskType()] = true
	}
	switch launch := registration.GetLaunch().(type) {
	case *contracts.AgentRegistration_KubernetesJob:
		if launch.KubernetesJob.GetImage() == "" || len(launch.KubernetesJob.GetCommand()) == 0 {
			return fmt.Errorf("agent %q requires image and command", id)
		}
	case *contracts.AgentRegistration_LocalProcess:
		if len(launch.LocalProcess.GetCommand()) == 0 {
			return fmt.Errorf("agent %q requires command", id)
		}
	default:
		return fmt.Errorf("agent %q requires a supported launch configuration", id)
	}
	return nil
}

// Resolve selects a worker only for a task it declared. The returned message
// is a clone, preserving the registry snapshot.
func (registry *Registry) Resolve(agentID, taskType string) (*contracts.AgentRegistration, error) {
	registration := registry.byID[agentID]
	if registration == nil {
		return nil, fmt.Errorf("unknown agent %q", agentID)
	}
	if capability(registration, taskType) == nil {
		return nil, fmt.Errorf("agent %q does not support task %q", agentID, taskType)
	}
	return proto.Clone(registration).(*contracts.AgentRegistration), nil
}

func capability(registration *contracts.AgentRegistration, taskType string) *contracts.AgentCapability {
	for _, candidate := range registration.GetCapabilities() {
		if candidate.GetTaskType() == taskType {
			return candidate
		}
	}
	return nil
}

// ValidateTask ensures that the payload type matches the selected worker's
// declared protobuf contract before any process or Job is launched.
func (registry *Registry) ValidateTask(task *contracts.AgentTask) error {
	if task == nil || task.GetSchemaVersion() != SchemaVersion || task.GetTaskId() == "" {
		return fmt.Errorf("agent task requires schema version %q and task ID", SchemaVersion)
	}
	registration, err := registry.Resolve(task.GetAgentId(), task.GetTaskType())
	if err != nil {
		return err
	}
	expected := capability(registration, task.GetTaskType()).GetRequestTypeUrl()
	if task.GetRequest() == nil || task.GetRequest().GetTypeUrl() != expected {
		return fmt.Errorf("agent task %q request type must be %q", task.GetTaskId(), expected)
	}
	return nil
}

// ValidateResult checks envelope identity and the response type. A failed or
// cancelled worker can return an error summary without a typed response.
func (registry *Registry) ValidateResult(task *contracts.AgentTask, result *contracts.AgentResult) error {
	if err := registry.ValidateTask(task); err != nil {
		return err
	}
	if result == nil || result.GetSchemaVersion() != SchemaVersion || result.GetTaskId() != task.GetTaskId() {
		return fmt.Errorf("agent result does not match task %q", task.GetTaskId())
	}
	if result.GetStatus() != "completed" && result.GetStatus() != "failed" && result.GetStatus() != "cancelled" {
		return fmt.Errorf("agent result %q has invalid status %q", task.GetTaskId(), result.GetStatus())
	}
	if result.GetStatus() == "completed" {
		registration, _ := registry.Resolve(task.GetAgentId(), task.GetTaskType())
		expected := capability(registration, task.GetTaskType()).GetResultTypeUrl()
		if result.GetResponse() == nil || result.GetResponse().GetTypeUrl() != expected {
			return fmt.Errorf("agent result %q response type must be %q", task.GetTaskId(), expected)
		}
	}
	if task.GetWorkspace() != nil && result.GetProposalId() != task.GetWorkspace().GetProposalId() {
		return fmt.Errorf("agent result %q proposal ID does not match task", task.GetTaskId())
	}
	return nil
}
