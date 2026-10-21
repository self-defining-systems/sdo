package runtime

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func TestIncidentWorkerEnvelopePreservesRequestAndResult(t *testing.T) {
	request := goldenRequest(t)
	task, payload, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t))
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
	cacheReads, uncached := int64(40), int64(50)
	typedResult.Usage = &contracts.UsageMetrics{InputTokens: 100, OutputTokens: 20, CacheReadInputTokens: &cacheReads, CacheWriteInputTokens: 10, UncachedInputTokens: &uncached}
	packed, err := anypb.New(typedResult)
	if err != nil {
		t.Fatal(err)
	}
	wireResult := &contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status), ProposalId: task.GetWorkspace().GetProposalId(), Response: packed}
	resultPayload, err := protojson.Marshal(wireResult)
	if err != nil {
		t.Fatal(err)
	}
	agentResult := &contracts.AgentResult{}
	if err := wire.Unmarshal(resultPayload, agentResult); err != nil {
		t.Fatal(err)
	}
	decoded, err := incidentResultFromAgent(agentResult, task, testIncidentRegistry(t, "test/image"))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.IncidentID != result.IncidentID || decoded.Status != result.Status {
		t.Fatalf("roundtrip changed result: %#v", decoded)
	}
	if decoded.Usage.GetCacheReadInputTokens() != 40 || decoded.Usage.GetUncachedInputTokens() != 50 {
		t.Fatalf("detailed usage disappeared at the worker boundary: %v", &decoded.Usage)
	}
}

func TestIncidentTaskCarriesLogicalMemoryAtBrokerBase(t *testing.T) {
	request := goldenRequest(t)
	snapshot := &contracts.OperationalMemorySnapshot{AcceptedCommit: request.RepositoryBaseCommit, GoalMarkdown: "# Goal", Playbooks: []*contracts.MemoryPlaybook{{Id: "repair-cache", Markdown: "# Repair"}}}
	task, _, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t), snapshot)
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
	if _, _, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t), snapshot); err == nil {
		t.Fatal("stale logical memory accepted")
	}
}

func TestIncidentTaskUsesCanonicalProtoJSONTimestamps(t *testing.T) {
	request := goldenRequest(t)
	request.ResponseDeadline = time.Date(2026, 10, 21, 4, 8, 25, 760700000, time.UTC)
	_, payload, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t))
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Request struct {
			ResponseDeadline string `json:"response_deadline"`
		} `json:"request"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if got := envelope.Request.ResponseDeadline; got != "2026-10-21T04:08:25.760700Z" {
		t.Fatalf("noncanonical ProtoJSON timestamp: %q", got)
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
