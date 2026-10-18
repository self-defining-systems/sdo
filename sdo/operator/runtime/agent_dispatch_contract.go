package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

const (
	incidentTaskType   = "incident-response"
	builtInResponderID = "sdo-responder"
)

func builtInResponderRegistration(image string, command []string) *contracts.AgentRegistration {
	return &contracts.AgentRegistration{
		SchemaVersion: ProtocolSchemaVersion,
		AgentId:       builtInResponderID,
		Capabilities: []*contracts.AgentCapability{{
			TaskType:       incidentTaskType,
			RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentRequest",
			ResultTypeUrl:  "type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult",
		}},
		Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: image, Command: command}},
	}
}

func builtInReflectorRegistration(image string) *contracts.AgentRegistration {
	return &contracts.AgentRegistration{
		SchemaVersion: ProtocolSchemaVersion,
		AgentId:       "sdo-reflector",
		Capabilities: []*contracts.AgentCapability{{
			TaskType:       "outcome-reflection",
			RequestTypeUrl: "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest",
			ResultTypeUrl:  "type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionResult",
		}},
		Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{
			Image: image, Command: []string{"python3", "-m", "sdo.agents.responder.reflection_worker"},
		}},
	}
}

func incidentRequestToProto(request IncidentRequest) (*contracts.IncidentRequest, error) {
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode incident request: %w", err)
	}
	raw, err = memory.NormalizeWorkerWire(raw)
	if err != nil {
		return nil, fmt.Errorf("logical incident playbooks: %w", err)
	}
	typed := &contracts.IncidentRequest{}
	if err := protojson.Unmarshal(raw, typed); err != nil {
		return nil, fmt.Errorf("convert incident request to protobuf: %w", err)
	}
	return typed, nil
}

func incidentResultToProto(result IncidentResult) (*contracts.IncidentResult, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode incident result: %w", err)
	}
	raw, err = memory.NormalizeWorkerWire(raw)
	if err != nil {
		return nil, fmt.Errorf("logical applied playbooks: %w", err)
	}
	typed := &contracts.IncidentResult{}
	if err := protojson.Unmarshal(raw, typed); err != nil {
		return nil, fmt.Errorf("convert incident result to protobuf: %w", err)
	}
	return typed, nil
}

func encodeIncidentAgentTask(request IncidentRequest, agentID string, snapshots ...*contracts.OperationalMemorySnapshot) (*contracts.AgentTask, []byte, error) {
	typedRequest, err := incidentRequestToProto(request)
	if err != nil {
		return nil, nil, err
	}
	if len(snapshots) > 1 {
		return nil, nil, fmt.Errorf("incident task accepts one memory snapshot")
	}
	if len(snapshots) == 1 {
		if snapshots[0] == nil || snapshots[0].GetAcceptedCommit() != request.RepositoryBaseCommit {
			return nil, nil, fmt.Errorf("memory snapshot differs from broker proposal base commit")
		}
		typedRequest.MemorySnapshot = snapshots[0]
	}
	packed, err := anypb.New(typedRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("pack incident request: %w", err)
	}
	task := &contracts.AgentTask{
		SchemaVersion: ProtocolSchemaVersion,
		TaskId:        request.IncidentID,
		AgentId:       agentID,
		TaskType:      incidentTaskType,
		Request:       packed,
		Workspace: &contracts.ProposalWorkspace{
			ProposalId:   request.IncidentID,
			CheckoutPath: request.RepositoryWorktree,
			BaseCommit:   request.RepositoryBaseCommit,
		},
	}
	payload, err := canonicalProtoJSON(task)
	if err != nil {
		return nil, nil, fmt.Errorf("encode agent task: %w", err)
	}
	if len(snapshots) == 1 && bytes.Contains(payload, []byte(".sdo/")) {
		return nil, nil, fmt.Errorf("physical operational memory path leaked into native incident task")
	}
	return task, payload, nil
}

func decodeIncidentAgentResult(payload []byte, task *contracts.AgentTask, registry *worker_registry.Registry) (IncidentResult, error) {
	result := &contracts.AgentResult{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payload, result); err != nil {
		return IncidentResult{}, fmt.Errorf("decode agent result: %w", err)
	}
	if err := registry.ValidateResult(task, result); err != nil {
		return IncidentResult{}, fmt.Errorf("validate agent result: %w", err)
	}
	if result.GetResponse() == nil {
		return IncidentResult{}, fmt.Errorf("agent task %q %s: %s", task.GetTaskId(), result.GetStatus(), result.GetErrorSummary())
	}
	typed := &contracts.IncidentResult{}
	if err := result.GetResponse().UnmarshalTo(typed); err != nil {
		return IncidentResult{}, fmt.Errorf("unpack incident result: %w", err)
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(typed)
	if err != nil {
		return IncidentResult{}, fmt.Errorf("encode typed incident result: %w", err)
	}
	// ProtoJSON emits int64 as strings; the older in-process result uses
	// numeric JSON. Keep the compatibility bridge at this dispatch edge.
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		return IncidentResult{}, fmt.Errorf("decode typed incident result JSON: %w", err)
	}
	if usage, ok := wire["usage"].(map[string]any); ok {
		for _, field := range []string{"llm_calls", "input_tokens", "output_tokens", "cached_input_tokens", "cache_read_input_tokens", "cache_write_input_tokens", "cache_write_1h_input_tokens", "uncached_input_tokens", "reasoning_output_tokens", "model_requests"} {
			if value, ok := usage[field].(string); ok {
				number, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return IncidentResult{}, fmt.Errorf("invalid usage %s: %w", field, err)
				}
				usage[field] = number
			}
		}
	}
	raw, err = json.Marshal(wire)
	if err != nil {
		return IncidentResult{}, fmt.Errorf("normalize typed incident result: %w", err)
	}
	var incidentResult IncidentResult
	if err := json.Unmarshal(raw, &incidentResult); err != nil {
		return IncidentResult{}, fmt.Errorf("convert incident result: %w", err)
	}
	return incidentResult, nil
}

// ProtoJSON does not promise stable field order. Canonicalizing lets a
// restarted controller compare its task with an existing ConfigMap exactly.
func canonicalProtoJSON(message proto.Message) ([]byte, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
