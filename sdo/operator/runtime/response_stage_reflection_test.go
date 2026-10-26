package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

func TestResponseStageClosedIncidentGetsFreshReflectionWithReports(t *testing.T) {
	root := nativeRepository(t)
	workspace := IncidentWorkspace{IncidentID: "automatic", Worktree: root, BaseCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	closure := nativeClosure(workspace)
	closure.Result.ResponderSessionID = ""
	closure.Result.ResolutionPath = "deterministic"
	closure.Request.PriorStages = []*contracts.StageReport{{Stage: "deterministic", Mitigations: []*contracts.StageMitigation{{PlaybookId: "repair", Result: "applied_cleared", ApplyReceipt: &contracts.PlanApplication{Applied: true, Plan: &contracts.Plan{}}}}}}
	calls := 0
	executor := reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.SessionMode != "fresh" || len(request.GetOutcome().GetPriorStages()) != 1 || len(request.GetClosure().GetRequest().GetPriorStages()) != 1 {
			t.Fatalf("reflection lost automatic evidence: %v", request)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "accepted playbook already covers this repair"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, Executor: executor}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: workspace.IncidentID, Classification: "success", PriorStages: cloneStageReports(closure.Request.PriorStages)}
	decision, err := worker.Reflect(context.Background(), closure, workspace, outcome, "repair", "outcome", ReflectionFeedback{})
	if err != nil || calls != 1 || decision.NoChangeReason == "no_session" {
		t.Fatalf("automatic reflection skipped: calls=%d decision=%+v err=%v", calls, decision, err)
	}
}
