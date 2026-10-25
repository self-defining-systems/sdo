package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
)

func TestEarlyClosureMissingReportResumesReflectionWithVerifiedReceipts(t *testing.T) {
	c, _, step := earlyWaitController(t, stateFinding("health"), stateFinding("health"), sdk.Finding{})
	request := *c.currentIncidentRequest
	loop := NewResponderLoop(request, nil, c.provider, nil, PlanExecutor{}, 3)
	loop.menuSession = &contracts.MitigationSession{ProviderSessionId: "early-responder-session"}
	finding := request.Findings[0]
	loop.menuRepairs = []RepairActionReceipt{{ActionID: "sdo-act-early", Kind: "kubectl-patch", Target: "Service/demo/api", Summary: "restore independently observed source setting", Details: "synthetic conditional patch and readback", Resources: []sdk.ObjectRef{finding.PrimaryResource}, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0), Success: true}}
	c.responderLoop = loop
	for i := 2; i <= 17; i++ {
		step(i)
	}
	closure, ok := c.PendingIncidentClosure()
	if !ok {
		t.Fatal("early closure missing")
	}
	if !hasResponderSession(closure) || len(closure.Result.RepairActions) != 1 || len(closure.Result.ConfirmedRootCauses) != 1 {
		t.Fatalf("missing durable receipt/session/cause: %+v", closure.Result)
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(validate.VerifiedReflectionCauses(derived.Record)) != 1 {
		t.Fatalf("receipt did not independently prove repair: %v", derived.Record)
	}
	root := nativeRepository(t)
	calls := 0
	executor := reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		calls++
		req := new(contracts.ReflectionRequest)
		if err := task.Request.UnmarshalTo(req); err != nil {
			t.Fatal(err)
		}
		if req.SessionMode != "resume" || req.Closure.Result.GetResponderSessionId() != "early-responder-session" || len(req.Outcome.ConfirmedRootCauses) != 1 || len(req.Outcome.RepairActions) != 1 {
			t.Fatalf("lost early closure reflection evidence: %v", req)
		}
		response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "sdk_gap", NoChangeReason: "scripted receipt boundary test", SdkGaps: []*contracts.SdkGap{{FaultClass: "synthetic", Primitive: "scripted-observation", Reason: "synthetic fixture has no authored executable", Resources: req.Outcome.ConfirmedRootCauses[0].Resources, ScratchScript: "scratch/test"}}})
		return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
	})
	worker := RegisteredReflectionWorker{TaskType: reflectionTaskType(t), AgentID: "reflector", Outcomes: memory.OpenReader(root), Timeout: time.Minute, Executor: executor, SessionExecutor: func(id string) (ReflectionTaskExecutor, error) {
		if id != request.IncidentID {
			t.Fatalf("session incident %s", id)
		}
		return executor, nil
	}}
	workspace := IncidentWorkspace{IncidentID: request.IncidentID, Worktree: root, BaseCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	closure.Request.RepositoryWorktree = filepath.Clean(root)
	closure.Request.RepositoryBaseCommit = workspace.BaseCommit
	if _, err := worker.Reflect(context.Background(), closure, workspace, derived.Record, "", "outcome", ReflectionFeedback{DispatchAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("reflection calls=%d", calls)
	}
}
