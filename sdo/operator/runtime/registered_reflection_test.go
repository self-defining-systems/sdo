package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

type reflectionTaskExecutorFunc func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)

func (f reflectionTaskExecutorFunc) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	return f(ctx, task)
}

func TestRegisteredReflectionWorkerUsesTypedTaskAndPriorMemory(t *testing.T) {
	store := memory.NewJSONStore(t.TempDir())
	if _, err := store.Put(context.Background(), []byte(`{"schema_version":1,"incident_id":"prior","classification":"success"}`)); err != nil {
		t.Fatal(err)
	}
	w := RegisteredReflectionWorker{AgentID: "reflector", Outcomes: store, Timeout: time.Minute}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if task.TaskType != "outcome-reflection" || task.Workspace.CheckoutPath != "/proposal" {
			t.Fatalf("wrong task: %+v", task)
		}
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.GetClosure().GetRequest().GetIncidentId() != "current" || len(request.PriorOutcomeJson) != 1 || request.ProposalCommit != "repair" || request.OutcomeCommit != "outcome" {
			t.Fatalf("wrong reflection request: %+v", request)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "prior case already covers it"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"})
	decision, err := w.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, []byte(`{"schema_version":1,"incident_id":"current","classification":"success"}`), "repair", "outcome")
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "no_change" {
		t.Fatalf("wrong decision: %+v", decision)
	}
}

func TestRegisteredReflectionWorkerResumeRequiresBoundSession(t *testing.T) {
	store := memory.NewJSONStore(t.TempDir())
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"})
	worker := RegisteredReflectionWorker{AgentID: "reflector", Outcomes: store, Timeout: time.Minute, SessionMode: "resume", Guidance: "generalize-spec"}
	called := false
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		called = true
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if request.GetSessionMode() != "resume" || request.GetGuidance() != "generalize-spec" || request.GetClosure().GetResult().GetResponderSessionId() != "session-123" {
			t.Fatalf("resume lost accepted session binding: %+v", request)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "already learned"})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	worker.SessionExecutor = func(id string) (ReflectionTaskExecutor, error) {
		if id != "current" {
			t.Fatalf("session transport received another incident: %q", id)
		}
		return worker.Executor, nil
	}
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, []byte(`{"schema_version":1,"incident_id":"current"}`), "repair", "outcome"); err == nil || called {
		t.Fatalf("resume without accepted session dispatched: %v", err)
	}
	closure.Result.ResponderSessionID = "../escape"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, []byte(`{"schema_version":1,"incident_id":"current"}`), "repair", "outcome"); err == nil || called {
		t.Fatalf("invalid session token dispatched: %v", err)
	}
	closure.Result.ResponderSessionID = "session-123"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, []byte(`{"schema_version":1,"incident_id":"current"}`), "repair", "outcome"); err != nil || !called {
		t.Fatalf("valid session was not dispatched: %v", err)
	}
}
