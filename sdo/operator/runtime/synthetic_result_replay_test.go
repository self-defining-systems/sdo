package runtime

import (
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
	"testing"
	"time"
)

func TestSyntheticWorkerResultsThroughContracts(t *testing.T) {
	registry, err := worker_registry.LoadDirectory(filepath.Join("..", "..", "agents", "registrations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		agent, taskType   string
		request, response proto.Message
	}{
		{"sdo-responder", "incident-response", &contracts.IncidentRequest{}, syntheticIncidentResult()},
		{"sdo-reflector", "outcome-reflection-direct-v1", &contracts.ReflectionRequest{}, &contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "synthetic fixture"}},
	} {
		t.Run(tc.agent, func(t *testing.T) {
			request, err := anypb.New(tc.request)
			if err != nil {
				t.Fatal(err)
			}
			response, err := anypb.New(tc.response)
			if err != nil {
				t.Fatal(err)
			}
			task := &contracts.AgentTask{SchemaVersion: ProtocolSchemaVersion, TaskId: "synthetic-task", AgentId: tc.agent, TaskType: tc.taskType, Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "synthetic-proposal"}}
			result := &contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
			wire, err := protojson.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			decoded := &contracts.AgentResult{}
			if err := protojson.Unmarshal(wire, decoded); err != nil {
				t.Fatal(err)
			}
			if err := registry.ValidateResult(task, decoded); err != nil {
				t.Fatal(err)
			}
			if tc.agent == "sdo-responder" {
				state, err := decodeIncidentAgentResult(wire, task, registry)
				if err != nil || state.IncidentID != "synthetic-incident" {
					t.Fatalf("decode response: %+v %v", state, err)
				}
			}
			decoded.ProposalId = "another-proposal"
			if err := registry.ValidateResult(task, decoded); err == nil {
				t.Fatal("cross-proposal result accepted")
			}
		})
	}
}

func syntheticIncidentResult() *contracts.IncidentResult {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &contracts.IncidentResult{SchemaVersion: ProtocolSchemaVersion, IncidentId: "synthetic-incident", Status: "completed", Usage: &contracts.UsageMetrics{}, Timing: &contracts.TimingMetrics{StartedAt: timestamppb.New(start), CompletedAt: timestamppb.New(start.Add(time.Second))}}
}
