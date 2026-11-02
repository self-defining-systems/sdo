//go:build integration

package runtime

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
)

func TestFailedClosureReflectionReceivesExactlyVerifiedCauses(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), ResponderBackend: "test", ResponderModel: "test", ForceReflection: true, Deriver: ProtoOutcomeDeriver{}}
	closure := failedThreeFaultClosure(t)
	workspace, err := native.PrepareIncident(ctx, closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	closure.Request.RepositoryWorktree = workspace.Worktree
	closure.Request.RepositoryBaseCommit = workspace.BaseCommit
	// The standalone controller fixture uses symbolic source IDs. Crossing the
	// real broker boundary requires the accepted repository's immutable commit.
	closure.Request.SourceCommit = workspace.BaseCommit
	closure.Request.RepairPolicy = "recorded-actions"
	calls := 0
	native.Reflector = RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.OpenReader(root), Timeout: time.Minute, SessionMode: "fresh", Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		request := new(contracts.ReflectionRequest)
		if err := task.Request.UnmarshalTo(request); err != nil {
			t.Fatal(err)
		}
		if request.Outcome.Classification != "failed" || len(request.Outcome.ConfirmedRootCauses) != 2 || len(request.Outcome.RepairActions) != 2 || len(request.Closure.Result.ConfirmedRootCauses) != 2 || len(request.CauseWork) != 2 || len(validate.VerifiedReflectionCauses(request.Outcome)) != 2 {
			t.Fatalf("incorrect filtered reflection input: %v", request)
		}
		if len(request.UnassignedWork) != 1 {
			t.Fatalf("missing unresolved-cause question: %v", request)
		}
		// Prose cannot cover recorded hand repairs, so the shared check rejects
		// this answer. Its diagnostics show which repairs carry authority.
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "filtered learning view retains two independently verified causes"})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})}
	if _, err := closeAndReflect(t, ctx, native, closure); err != nil {
		t.Fatal(err)
	}
	ledger, err := native.readLedger(closure.Request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if calls == 0 || calls != len(ledger.ReflectionRejections) || !ledger.ReflectionRejected || ledger.OutcomeDocument.Classification != "failed" || len(ledger.OutcomeDocument.ConfirmedRootCauses) != 3 {
		t.Fatalf("durable outcome altered or rejection not recorded: calls=%d %v", calls, ledger)
	}
	for _, rejection := range ledger.ReflectionRejections {
		var named []string
		for _, d := range rejection.Diagnostics {
			if d.Rule != validate.RuleReflectionProgramRequired {
				t.Fatalf("unexpected rejection: %v", d)
			}
			named = append(named, d.Message)
		}
		text := strings.Join(named, "\n")
		if len(named) != 2 || !strings.Contains(text, `"sdo-act-first"`) || !strings.Contains(text, `"sdo-act-second"`) || strings.Contains(text, "blocker") {
			t.Fatalf("only independently verified repairs require coverage: %v", named)
		}
	}
}
