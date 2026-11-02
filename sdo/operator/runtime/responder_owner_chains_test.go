package runtime

import (
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
)

// REV12 F1: a worker can write any owner chain into its result. Cause
// attribution must follow only owner references SDO observed itself.
func TestWorkerReportedOwnerChainsAreIgnored(t *testing.T) {
	request := goldenRequest(t)
	task, _, err := encodeIncidentAgentTask(request, testIncidentRegistry(t, ""), testIncidentAgentID(t))
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 26, 15, 18, 0, 0, time.UTC)
	unrelated := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "unrelated-7c9d"}
	result := completedResult(request.IncidentID)
	result.RepairActions = []RepairActionReceipt{{
		ScriptDigest: "forged-script-digest", ActionID: "sdo-act-forged", Kind: "kubectl-delete", Target: "Pod/unrelated-7c9d", Resources: []sdk.ObjectRef{unrelated},
		Summary: "delete", Details: "delete", StartedAt: at, CompletedAt: at.Add(time.Second), Success: true,
		Owners: []ResourceOwners{{Resource: unrelated, OwnerChain: []sdk.ObjectRef{{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cause"}}}},
	}}
	typed, err := incidentResultToProto(result)
	if err != nil {
		t.Fatal(err)
	}
	if len(typed.GetRepairActions()[0].GetOwners()) != 1 {
		t.Fatal("fixture lost its forged chain before the worker boundary")
	}
	packed, err := anypb.New(typed)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protojson.Marshal(&contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.GetTaskId(), Status: string(result.Status), ProposalId: task.GetWorkspace().GetProposalId(), Response: packed})
	if err != nil {
		t.Fatal(err)
	}
	agentResult := &contracts.AgentResult{}
	if err := wire.Unmarshal(payload, agentResult); err != nil {
		t.Fatal(err)
	}
	decoded, err := incidentResultFromAgent(agentResult, task, testIncidentRegistry(t, "test/image"))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.RepairActions) != 1 || len(decoded.RepairActions[0].Owners) != 0 || decoded.RepairActions[0].ScriptDigest != "" {
		t.Fatalf("worker-reported owner chain reached SDO: %#v", decoded.RepairActions)
	}
}
