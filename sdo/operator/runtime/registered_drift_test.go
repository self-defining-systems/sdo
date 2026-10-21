package runtime

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

func TestRegisteredDriftInvestigatorUsesTypedCapability(t *testing.T) {
	registry, err := worker_registry.New(builtInDriftRegistration("test/image"))
	if err != nil {
		t.Fatal(err)
	}
	worker := RegisteredDriftInvestigator{AgentID: "sdo-drift-investigator", Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if err := registry.ValidateTask(task); err != nil {
			t.Fatal(err)
		}
		if task.GetTaskType() != driftTaskType || task.GetWorkspace().GetBaseCommit() != "accepted" {
			t.Fatalf("wrong drift task: %+v", task)
		}
		result, err := anypb.New(&contracts.DriftInvestigationResult{Events: []*contracts.DriftEventInvestigation{{Verdict: "unsure", Reason: "no relation evidence"}}})
		if err != nil {
			return nil, err
		}
		response := &contracts.AgentResult{SchemaVersion: ProtocolSchemaVersion, TaskId: task.TaskId, Status: "completed", ProposalId: task.Workspace.ProposalId, Response: result}
		if err := registry.ValidateResult(task, response); err != nil {
			t.Fatal(err)
		}
		return response, nil
	})}
	request := &contracts.DriftInvestigationTask{BatchId: "drift-1", AcceptedCommit: "accepted", RepositoryWorktree: "/workspace/app",
		MemorySnapshot: &contracts.OperationalMemorySnapshot{AcceptedCommit: "accepted"}}
	result, err := worker.Investigate(context.Background(), request)
	if err != nil || result.GetEvents()[0].GetVerdict() != "unsure" {
		t.Fatalf("typed drift result lost: %+v, %v", result, err)
	}
	request.MemorySnapshot.AcceptedCommit = "stale"
	if _, err := worker.Investigate(context.Background(), request); err == nil {
		t.Fatal("stale memory snapshot was accepted")
	}
}

func TestDriftSessionToolsRejectMutation(t *testing.T) {
	handle := driftReadOnlyToolHandle(nil)
	for _, command := range []string{"plan", "apply", "verify", "rollback"} {
		if response := handle(context.Background(), ToolRequest{Command: command}); response.Error == "" {
			t.Fatalf("drift tool %q was allowed", command)
		}
	}
}
