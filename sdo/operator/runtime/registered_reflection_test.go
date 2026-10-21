package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry/registrytest"
)

type reflectionTaskExecutorFunc func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)

func (f reflectionTaskExecutorFunc) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	return f(ctx, task)
}

func TestRegisteredReflectionWorkerUsesTypedTaskAndPriorMemory(t *testing.T) {
	store := memory.NewJSONStore(t.TempDir())
	if _, err := store.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "prior", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	w := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: store, Timeout: time.Minute}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if task.TaskType != reflectionTaskType(t) || task.Workspace.CheckoutPath != "/proposal" {
			t.Fatalf("wrong task: %+v", task)
		}
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.GetClosure().GetRequest().GetIncidentId() != "current" || len(request.GetPriorOutcomes()) != 1 || request.ProposalCommit != "repair" || request.OutcomeCommit != "outcome" {
			t.Fatalf("wrong reflection request: %+v", request)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "prior case already covers it"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"})
	decision, err := w.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
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
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: store, Timeout: time.Minute, SessionMode: "resume", Guidance: "generalize-spec"}
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
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err == nil || called {
		t.Fatalf("resume without accepted session dispatched: %v", err)
	}
	closure.Result.ResponderSessionID = "../escape"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err == nil || called {
		t.Fatalf("invalid session token dispatched: %v", err)
	}
	closure.Result.ResponderSessionID = "session-123"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: "/proposal", BaseCommit: "base"}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err != nil || !called {
		t.Fatalf("valid session was not dispatched: %v", err)
	}
}

// reflectionTaskType is the task type the built-in manifests register for
// reflection requests.
func reflectionTaskType(t testing.TB) string {
	t.Helper()
	registry := registrytest.BuiltIn(t)
	agent, err := registry.AgentFor(&contracts.ReflectionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return registrytest.TaskType(t, agent, &contracts.ReflectionRequest{})
}

func TestRegisteredReflectionWorkerDirectEdits(t *testing.T) {
	for _, tc := range []struct {
		name, decision, reason  string
		edit, legacy, wantError bool
	}{
		{name: "update", decision: "updated", edit: true},
		{name: "no change", decision: "no_change", reason: "already covered"},
		{name: "SDK gap", decision: "sdk_gap", reason: "SDK lacks the observation"},
		{name: "hidden edit", decision: "no_change", reason: "already covered", edit: true, wantError: true},
		{name: "empty update", decision: "updated", wantError: true},
		{name: "legacy proposal", decision: "updated", edit: true, legacy: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			worker := RegisteredReflectionWorker{AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute}
			worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				if task.TaskType != "outcome-reflection-direct-v1" {
					t.Fatalf("legacy capability: %s", task.TaskType)
				}
				if tc.edit {
					if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/base/README.md"), []byte("updated"), 0o644); err != nil {
						return nil, err
					}
				}
				response := &contracts.ReflectionResult{LearningDecision: tc.decision, NoChangeReason: tc.reason}
				if tc.edit {
					response.ProposedArtifactIds = []string{"base"}
				}
				if tc.legacy {
					response.ArtifactProposal = &contracts.OperationalArtifactProposal{}
				}
				packed, err := anypb.New(response)
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: packed}, err
			})
			workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
			_, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current"}, "repair", base)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}
