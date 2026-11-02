//go:build integration

package runtime

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

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
