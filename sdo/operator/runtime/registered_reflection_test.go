package runtime

import (
	"bytes"
	"context"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry/registrytest"
)

func TestRegisteredReflectionWorkerExportsTurnUsage(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	logPath := filepath.Join(t.TempDir(), "reflection-usage.jsonl")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh", UsageLogPath: func(string) string { return logPath }}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if err := os.WriteFile(logPath, []byte(`{"usage":{"input_tokens":42,"output_tokens":7,"model_requests":2}}`+"\n"), 0600); err != nil {
			return nil, err
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Usage == nil || decision.Usage.InputTokens != 42 || decision.Usage.OutputTokens != 7 || decision.Usage.GetModelRequests() != 2 {
		t.Fatalf("reflection usage lost: %+v", decision.Usage)
	}
}

func TestRegisteredReflectionWorkerReturnsTypedFailure(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "failed", ErrorSummary: "watch namespace is not a proto field"}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	decision, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil || decision.LearningDecision != "failed" || decision.Error != "watch namespace is not a proto field" {
		t.Fatalf("typed failure lost: %+v, %v", decision, err)
	}
}

func TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: strings.Repeat("x", 700<<10)})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	_, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err == nil || !strings.Contains(err.Error(), "transport limit") {
		t.Fatalf("oversized worker result passed reflection validation: %v", err)
	}
}

func TestRegisteredReflectionWorkerReturnsTypedSDKGap(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh"}
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		response, err := anypb.New(&contracts.ReflectionResult{
			LearningDecision: "sdk_gap", NoChangeReason: "The incident requires a UDP probe",
			SdkGaps: []*contracts.SdkGap{{FaultClass: "service-port-mismatch", Primitive: "ProbeUDP", Reason: "The incident required a UDP probe", ScratchScript: "kubectl run probe"}},
		})
		if err != nil {
			return nil, err
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "sdk_gap" || len(decision.SDKGaps) != 1 || decision.SDKGaps[0].Primitive != "ProbeUDP" {
		t.Fatalf("SDK gap lost: %+v", decision)
	}
}

type reflectionTaskExecutorFunc func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error)

func (f reflectionTaskExecutorFunc) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	return f(ctx, task)
}

func TestRegisteredReflectionWorkerUsesTypedTaskAndPriorMemory(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	if _, err := store.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "prior", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	w := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		if task.TaskType != reflectionTaskType(t) || task.Workspace.CheckoutPath != root {
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
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	decision, err := w.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{})
	if err != nil {
		t.Fatal(err)
	}
	if decision.LearningDecision != "no_change" {
		t.Fatalf("wrong decision: %+v", decision)
	}
}

func TestRegisteredReflectionWorkerResumeFallsBackWithoutBoundSession(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	closure := nativeClosure(IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base})
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: store, Timeout: time.Minute, SessionMode: "resume", Guidance: "generalize-spec"}
	called := false
	var modes []string
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		called = true
		request := &contracts.ReflectionRequest{}
		if err := task.Request.UnmarshalTo(request); err != nil {
			return nil, err
		}
		if request.GetSessionMode() != "resume" && request.GetSessionMode() != "fresh" || request.GetGuidance() != "generalize-spec" {
			t.Fatalf("resume lost accepted session binding: %+v", request)
		}
		modes = append(modes, request.GetSessionMode())
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
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err != nil || !called {
		t.Fatalf("missing session did not fall back: %v", err)
	}
	called = false
	closure.Result.ResponderSessionID = "../escape"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err != nil || !called {
		t.Fatalf("invalid session token did not fall back: %v", err)
	}
	closure.Result.ResponderSessionID = "session-123"
	if _, err := worker.Reflect(context.Background(), closure, IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", "outcome", ReflectionFeedback{}); err != nil || !called {
		t.Fatalf("valid session was not dispatched: %v", err)
	}
	if len(modes) != 3 || modes[0] != "fresh" || modes[1] != "fresh" || modes[2] != "resume" {
		t.Fatalf("wrong fallback modes: %v", modes)
	}
	if !bytes.Contains(logs.Bytes(), []byte("session ID is missing or invalid; starting fresh")) {
		t.Fatalf("fallback reason was not logged: %s", logs.String())
	}
}

func TestRegisteredReflectionRetryUsesANewTaskIdentity(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	store := memory.NewJSONStore(t.TempDir())
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: store, Timeout: time.Minute}
	var ids []string
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		ids = append(ids, task.TaskId)
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	record := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}
	for _, attempt := range []int{0, 0, 1} {
		feedback := ReflectionFeedback{Attempt: uint32(attempt + 1)}
		if attempt > 0 {
			feedback.Diagnostics = []*contracts.MemoryDiagnostic{{Rule: "test", Message: "retry"}}
		}
		if _, err := worker.Reflect(context.Background(), closure, workspace, record, "repair", "outcome", feedback); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] != ids[1] {
		t.Fatalf("the same attempt must keep its identity so a restart resumes its Job: %v", ids)
	}
	if ids[2] == ids[0] {
		t.Fatalf("a retry after a rejected reflection must not reuse the rejected attempt's Job and result: %v", ids)
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
		{name: "SDK gap without reason", decision: "sdk_gap", wantError: true},
		{name: "hidden edit", decision: "no_change", reason: "already covered", edit: true},
		{name: "empty update", decision: "updated"},
		{name: "legacy proposal", decision: "updated", edit: true, legacy: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeRepository(t)
			nativeMemoryFixture(t, root)
			base := nativeGit(t, root, "rev-parse", "HEAD")
			worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute}
			worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				if task.TaskType != reflectionTaskType(t) {
					t.Fatalf("legacy capability: %s", task.TaskType)
				}
				if tc.edit {
					if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/base/README.md"), []byte("updated"), 0o644); err != nil {
						return nil, err
					}
				}
				response := &contracts.ReflectionResult{LearningDecision: tc.decision, NoChangeReason: tc.reason}
				if tc.decision == "sdk_gap" {
					response.SdkGaps = []*contracts.SdkGap{{FaultClass: "fault", Primitive: "ProbeUDP", Reason: "UDP", ScratchScript: "probe"}}
				}
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
			_, err := worker.Reflect(context.Background(), nativeClosure(workspace), workspace, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}, "repair", base, ReflectionFeedback{})
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}
