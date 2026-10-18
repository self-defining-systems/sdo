package runtime

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

func TestIncidentWorkerEnvelopePreservesRequestAndResult(t *testing.T) {
	request := goldenRequest(t)
	task, payload, err := encodeIncidentAgentTask(request, "sdo-responder")
	if err != nil {
		t.Fatal(err)
	}
	var wireTask contracts.AgentTask
	if err := protojson.Unmarshal(payload, &wireTask); err != nil {
		t.Fatal(err)
	}
	if wireTask.GetTaskId() != request.IncidentID || wireTask.GetWorkspace().GetCheckoutPath() != request.RepositoryWorktree {
		t.Fatalf("unexpected task envelope: %v", &wireTask)
	}
	var typedRequest contracts.IncidentRequest
	if err := wireTask.GetRequest().UnmarshalTo(&typedRequest); err != nil {
		t.Fatal(err)
	}
	if typedRequest.GetIncidentId() != request.IncidentID {
		t.Fatalf("lost incident ID: %v", typedRequest.GetIncidentId())
	}

	result := completedResult(request.IncidentID)
	typedResult, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := anypb.New(typedResult)
	if err != nil {
		t.Fatal(err)
	}
	wireResult := &contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status), ProposalId: task.GetWorkspace().GetProposalId(), Response: packed}
	resultPayload, err := protojson.Marshal(wireResult)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := worker_registry.New(builtInResponderRegistration("test/image", []string{"/worker"}))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeIncidentAgentResult(resultPayload, task, registry)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.IncidentID != result.IncidentID || decoded.Status != result.Status {
		t.Fatalf("roundtrip changed result: %#v", decoded)
	}
}

func TestIncidentTaskCarriesLogicalMemoryAtBrokerBase(t *testing.T) {
	request := goldenRequest(t)
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: request.RepositoryBaseCommit, GoalMarkdown: "# Goal", Playbooks: []*contracts.MemoryPlaybook{{Id: "repair-cache", Markdown: "# Repair"}}}
	task, _, err := encodeIncidentAgentTask(request, "sdo-responder", snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var typed contracts.IncidentRequest
	if err := task.GetRequest().UnmarshalTo(&typed); err != nil {
		t.Fatal(err)
	}
	if typed.GetMemorySnapshot().GetPlaybooks()[0].GetId() != "repair-cache" {
		t.Fatal("logical memory omitted from task")
	}
	snapshot.AcceptedCommit = "different"
	if _, _, err := encodeIncidentAgentTask(request, "sdo-responder", snapshot); err == nil {
		t.Fatal("stale logical memory accepted")
	}
}

func TestIncidentResultWireUsesLogicalPlaybookAndScriptIDs(t *testing.T) {
	result := completedResult(goldenRequest(t).IncidentID)
	result.AppliedPlaybooks = []AppliedPlaybook{{PlaybookID: "repair-cache", ScriptNames: []string{"verify.sh"}}}
	result.ProposedArtifactIDs = []string{"repair-cache"}
	typed, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	if got := typed.GetAppliedPlaybooks()[0]; got.GetPlaybookId() != "repair-cache" || len(got.GetScriptNames()) != 1 || got.GetScriptNames()[0] != "verify.sh" {
		t.Fatalf("physical playbook reference leaked into worker result: %v", got)
	}
	if ids := typed.GetProposedArtifactIds(); len(ids) != 1 || ids[0] != "repair-cache" {
		t.Fatalf("outcome storage path leaked into proposed artifacts: %v", ids)
	}
}
