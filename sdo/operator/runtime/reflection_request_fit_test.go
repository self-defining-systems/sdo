package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// closeWithReflection closes a verified-success incident whose outcome is shaped by shape, through the registered
// reflection worker, and returns the broker, the receipt and the tasks the executor saw.
func closeWithReflection(t *testing.T, shape func(*contracts.OutcomeRecord)) (NativeIncidentBroker, IncidentWorkspace, ClosureReceipt, []*contracts.AgentTask, error) {
	t.Helper()
	accepted := nativeRepository(t)
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	var dispatched []*contracts.AgentTask
	worker := RegisteredReflectionWorker{
		TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh",
		Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
			dispatched = append(dispatched, task)
			response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
			if err != nil {
				t.Fatal(err)
			}
			return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
		}),
	}
	native := NativeIncidentBroker{
		Repository: repository, LedgerRoot: filepath.Join(t.TempDir(), "ledger"), ResponderBackend: "test", ResponderModel: "test", Reflector: worker,
		Deriver: OutcomeDeriverFunc(func(_ context.Context, closure IncidentClosure, _, _, _ string) (DerivedOutcome, error) {
			derived := testDerivedOutcome(closure.Request.IncidentID, "success", true)
			shape(derived.Record)
			return derived, nil
		}),
	}
	workspace, err := native.PrepareIncident(context.Background(), "inc-over-limit")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Worktree, "app.txt"), []byte("repair\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	closure := nativeClosure(workspace)
	closure.VerifiedAt = time.Now().UTC()
	receipt, err := native.ProcessClosure(context.Background(), closure)
	return native, workspace, receipt, dispatched, err
}

// A transport limit never blocks closure or learning silently: an over-limit request is degraded in a fixed order,
// each step recorded on the frozen task, and the reflection still runs.
func TestOverLimitReflectionDegradesAndRunsThroughClosure(t *testing.T) {
	_, _, receipt, dispatched, err := closeWithReflection(t, func(outcome *contracts.OutcomeRecord) {
		for i := 0; i < 200; i++ {
			outcome.ProgramToolCalls = append(outcome.ProgramToolCalls, &contracts.ProgramToolCall{Command: "tool", ResponseJson: strings.Repeat("r", 4000)})
		}
	})
	if err != nil || receipt.AckToken == "" {
		t.Fatalf("closure failed on a transport limit: receipt=%+v err=%v", receipt, err)
	}
	if len(dispatched) != 1 {
		t.Fatalf("degraded reflection was not dispatched once: %d", len(dispatched))
	}
	request := &contracts.ReflectionRequest{}
	if err := dispatched[0].Request.UnmarshalTo(request); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(request.GetCompaction(), ","); got != "cap_program_runs_and_tool_calls" {
		t.Fatalf("compaction steps = %q", got)
	}
	if size := proto.Size(dispatched[0]); size > worker_registry.MaxRequestBytes {
		t.Fatalf("degraded task is %d bytes, over the limit", size)
	}
}

// A request no degrade step can shrink skips reflection with a typed, non-retryable reason; the incident closes.
func TestHopelessOverLimitReflectionIsSkippedTypedAndTheIncidentCloses(t *testing.T) {
	native, workspace, receipt, dispatched, err := closeWithReflection(t, func(outcome *contracts.OutcomeRecord) {
		outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: strings.Repeat("c", 700<<10), Resources: []*contracts.ObjectRef{{Kind: "Deployment", Name: "app"}}}}
	})
	if err != nil || receipt.AckToken == "" {
		t.Fatalf("closure failed on a transport limit: receipt=%+v err=%v", receipt, err)
	}
	if len(dispatched) != 0 {
		t.Fatalf("a hopeless request reached a worker: %d tasks", len(dispatched))
	}
	ledger, err := native.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if !ledger.ReflectionProcessed || ledger.ReflectionSkippedReason != ReflectionRequestTooLarge {
		t.Fatalf("skip not recorded: processed=%v reason=%q", ledger.ReflectionProcessed, ledger.ReflectionSkippedReason)
	}
}

func TestTransportLimitErrorIsNonRetryable(t *testing.T) {
	var err error = &worker_registry.RequestTooLargeError{}
	if terminal, ok := err.(interface{ NonRetryable() bool }); !ok || !terminal.NonRetryable() {
		t.Fatal("a request that does not fit must not be retried")
	}
}

// A task frozen by an earlier build with no limit is rebuilt within the limit instead of failing every attempt.
func TestFrozenOverLimitReflectionTaskIsRebuilt(t *testing.T) {
	root := nativeRepository(t)
	base := nativeGit(t, root, "rev-parse", "HEAD")
	taskStore := &lifecycleio.TaskStore{Root: t.TempDir()}
	var dispatched []*contracts.AgentTask
	failFirst := true
	w := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute, SessionMode: "fresh", TaskStore: taskStore}
	w.Executor = reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		dispatched = append(dispatched, task)
		if failFirst {
			failFirst = false
			return nil, context.Canceled
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
		if err != nil {
			t.Fatal(err)
		}
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, nil
	})
	workspace := IncidentWorkspace{IncidentID: "current", Worktree: root, BaseCommit: base}
	closure := nativeClosure(workspace)
	closure.VerifiedAt = time.Now().UTC()
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "current", Classification: "success"}
	if _, err := w.Reflect(context.Background(), closure, workspace, outcome, "repair", "outcome", ReflectionFeedback{}); err == nil {
		t.Fatal("first attempt should have failed")
	}
	frozen, _, err := taskStore.Load(dispatched[0].TaskId)
	if err != nil || frozen == nil {
		t.Fatalf("no frozen task: %v", err)
	}
	request := &contracts.ReflectionRequest{}
	if err := frozen.Request.UnmarshalTo(request); err != nil {
		t.Fatal(err)
	}
	request.Outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: strings.Repeat("c", 700<<10)}}
	if frozen.Request, err = anypb.New(request); err != nil {
		t.Fatal(err)
	}
	if err := taskStore.Discard(frozen.TaskId); err != nil {
		t.Fatal(err)
	}
	if err := taskStore.Save(frozen); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Reflect(context.Background(), closure, workspace, outcome, "repair", "outcome", ReflectionFeedback{}); err != nil {
		t.Fatalf("an old oversized frozen task failed the attempt: %v", err)
	}
	if last := dispatched[len(dispatched)-1]; proto.Size(last) > worker_registry.MaxRequestBytes {
		t.Fatalf("the rebuilt task is still %d bytes", proto.Size(last))
	}
}
