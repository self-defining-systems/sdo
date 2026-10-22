package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func TestReflectionRetryReusesFrozenTaskDespiteChangedClockAndMemory(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	workspace := IncidentWorkspace{IncidentID: "frozen", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	outcomes := memory.NewJSONStore(t.TempDir())
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: outcomes, Timeout: time.Minute, TaskStore: &lifecycleio.TaskStore{Root: t.TempDir()}}
	var first *contracts.AgentTask
	calls := 0
	worker.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		if first == nil {
			first = proto.Clone(task).(*contracts.AgentTask)
			return nil, errors.New("result read unavailable")
		}
		if !proto.Equal(first, task) {
			t.Fatal("retry rebuilt task payload")
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	record := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "frozen", Classification: "success"}
	feedback := ReflectionFeedback{DispatchAt: time.Unix(1000, 0)}
	if _, err := worker.Reflect(context.Background(), closure, workspace, record, "repair", "outcome", feedback); err == nil {
		t.Fatal("expected result read fault")
	}
	if _, err := outcomes.Put(context.Background(), &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "new-prior", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	feedback.DispatchAt = feedback.DispatchAt.Add(time.Hour)
	worker.Timeout = 2 * time.Hour
	worker.Guidance = "changed after restart"
	if _, err := worker.Reflect(context.Background(), closure, workspace, record, "repair", "outcome", feedback); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestNativeBrokerPicksUpCompletedReflectionAfterResultReadFailure(t *testing.T) {
	h := newRejectionHarness(t, "completed-result-read", nil)
	var frozen *contracts.AgentTask
	calls := 0
	h.native.Reflector = RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, TaskStore: &lifecycleio.TaskStore{Root: t.TempDir()}, Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		if frozen == nil {
			frozen = proto.Clone(task).(*contracts.AgentTask)
			if _, err := writeGoodPlaybook(IncidentWorkspace{Worktree: task.Workspace.CheckoutPath}); err != nil {
				return nil, err
			}
			return nil, errors.New("completed Job result unavailable")
		}
		if !proto.Equal(frozen, task) {
			t.Fatal("completed Job was redispatched with new bytes")
		}
		if _, err := os.Stat(filepath.Join(task.Workspace.CheckoutPath, ".sdo/playbooks/base/README.md")); err != nil {
			t.Fatalf("completed checkout discarded: %v", err)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})}
	if _, err := h.native.ProcessClosure(context.Background(), h.closure); err == nil {
		t.Fatal("expected completed-result read fault")
	}
	receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
	if err != nil || receipt.ReflectionCommit == "" || calls != 2 {
		t.Fatalf("completed result recovery: receipt=%+v calls=%d err=%v", receipt, calls, err)
	}
	ledger, err := h.native.readLedger(h.workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ledger.ReflectionResults) != 1 || len(ledger.ReflectionRejections) != 0 {
		t.Fatalf("completed result was lost or rejected: %v", ledger)
	}
}
