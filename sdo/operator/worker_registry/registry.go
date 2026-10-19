// Package worker_registry resolves registered agent workers by their declared
// task and protobuf message types. It knows launch artifacts, not worker code.
package worker_registry

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// SchemaVersion is the protocol version the contract messages declare as the
// const of AgentTask.schema_version; the proto is its only definition.
var SchemaVersion = wire.StringConst(&contracts.AgentTask{}, "schema_version")

// TypeURL is the Any type URL of message.
func TypeURL(message proto.Message) string {
	return "type.googleapis.com/" + string(message.ProtoReflect().Descriptor().FullName())
}

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

// DefaultDirectory is where the controller and worker images carry the
// built-in agent manifests (sdo/agents/registrations at build time).
const DefaultDirectory = "/opt/sdo/sdo/agents/registrations"

// LoadBuiltIn loads the built-in manifests from directory, or DefaultDirectory
// when it is empty; tests point it at the checked-in manifests. A non-empty
// image replaces their Kubernetes Job launch image, which is how a deployment
// points them at the worker image it built.
func LoadBuiltIn(directory, image string) (*Registry, error) {
	if directory == "" {
		directory = DefaultDirectory
	}
	registry, err := LoadDirectory(directory)
	if err != nil {
		return nil, fmt.Errorf("load built-in agent registrations (pass --agent-registration-directory to use another directory): %w", err)
	}
	if image == "" {
		return registry, nil
	}
	return registry.WithLaunchImage(image)
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
		if err := wire.Unmarshal(data, registration); err != nil {
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
	return wire.Validate(registration)
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
	if task == nil {
		return fmt.Errorf("nil agent task")
	}
	if err := wire.Validate(task); err != nil {
		return err
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
	if result == nil {
		return fmt.Errorf("nil agent result for task %q", task.GetTaskId())
	}
	if err := wire.Validate(result); err != nil {
		return err
	}
	if result.GetTaskId() != task.GetTaskId() {
		return fmt.Errorf("agent result does not match task %q", task.GetTaskId())
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

// TaskType returns the task type agentID declared for request's message type.
// Task types are defined by the registration manifests alone; code that
// dispatches a request names the message, never the task-type string.
func (registry *Registry) TaskType(agentID string, request proto.Message) (string, error) {
	registration := registry.byID[agentID]
	if registration == nil {
		return "", fmt.Errorf("unknown agent %q", agentID)
	}
	for _, candidate := range registration.GetCapabilities() {
		if candidate.GetRequestTypeUrl() == TypeURL(request) {
			return candidate.GetTaskType(), nil
		}
	}
	return "", fmt.Errorf("agent %q declares no task for %s", agentID, request.ProtoReflect().Descriptor().FullName())
}

// AgentFor returns the one registered agent that declares a task for request's
// message type, so a caller that needs "the responder" or "the reflector"
// names the contract rather than an agent ID.
func (registry *Registry) AgentFor(request proto.Message) (string, error) {
	var found []string
	for id, registration := range registry.byID {
		for _, candidate := range registration.GetCapabilities() {
			if candidate.GetRequestTypeUrl() == TypeURL(request) {
				found = append(found, id)
				break
			}
		}
	}
	sort.Strings(found)
	switch len(found) {
	case 1:
		return found[0], nil
	case 0:
		return "", fmt.Errorf("no registered agent declares a task for %s", request.ProtoReflect().Descriptor().FullName())
	default:
		return "", fmt.Errorf("agents %v all declare a task for %s; select one explicitly", found, request.ProtoReflect().Descriptor().FullName())
	}
}

// WithLaunchImage returns a registry whose Kubernetes Job registrations launch
// image. It is how a deployment points the built-in manifests at the worker
// image it built.
func (registry *Registry) WithLaunchImage(image string) (*Registry, error) {
	registrations := make([]*contracts.AgentRegistration, 0, len(registry.byID))
	for _, registration := range registry.byID {
		clone := proto.Clone(registration).(*contracts.AgentRegistration)
		if job := clone.GetKubernetesJob(); job != nil {
			job.Image = image
		}
		registrations = append(registrations, clone)
	}
	return New(registrations...)
}
