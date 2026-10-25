package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
)

// socialRepeatClosure is handwritten toy evidence with the S42 social-repeat
// shape: reported successful actions, aggregate closure failed, no independent
// per-cause verification. It contains no captured output or harness verdict.
func socialRepeatClosure(workspace IncidentWorkspace) IncidentClosure {
	c := nativeClosure(workspace)
	c.Request.RepairPolicy = "recorded-actions"
	c.Result.ResponderSessionID = "responder-social-repeat"
	c.Result.Status = IncidentFailed
	c.FinalDetectorStates = nil
	c.Request.ResponseDeadline = c.ResponderCompletedAt.Add(-time.Second)
	c.Result.ConfirmedRootCauses = []ConfirmedRootCause{{
		Summary: "unsupported scheduling constraint", Resources: []sdk.ObjectRef{{Kind: "Deployment", Namespace: "default", Name: "toy-user"}},
		Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "responder", Observation: "workload became ready after removing selector"}},
	}, {
		Summary: "admission trust configuration", Resources: []sdk.ObjectRef{{Kind: "Deployment", Namespace: "default", Name: "toy-front"}},
		Evidence: []RootCauseEvidence{{Kind: "live-observation", Source: "responder", Observation: "pod creation resumed after trust repair"}},
	}}
	c.Result.RepairActions = []RepairActionReceipt{{ActionID: "recorded-selector-repair", Kind: "kubectl-patch", Target: "Deployment/default/toy-user", Summary: "remove unsupported selector", Details: "command succeeded", Success: true, Resources: c.Result.ConfirmedRootCauses[0].Resources, StartedAt: c.DetectedAt, CompletedAt: c.ResponderCompletedAt}}
	return c
}

func TestReflectionEveryClosedResponderIncident(t *testing.T) {
	for _, scenario := range []string{"failed-social-repeat", "cancelled-self-healed", "failed-no-causes"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			root := nativeRepository(t)
			repository, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
			if err != nil {
				t.Fatal(err)
			}
			native := NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
			workspace, err := native.PrepareIncident(ctx, scenario)
			if err != nil {
				t.Fatal(err)
			}
			closure := socialRepeatClosure(workspace)
			if scenario == "cancelled-self-healed" {
				closure.Result.Status = IncidentCancelled
				closure.Result.ConfirmedRootCauses, closure.Result.RepairActions = nil, nil
				closure.FinalDetectorStates = nativeClosure(workspace).FinalDetectorStates
			}
			if scenario == "failed-no-causes" {
				closure.Result.ConfirmedRootCauses, closure.Result.RepairActions = nil, nil
			}
			calls := 0
			native.Reflector = RegisteredReflectionWorker{
				AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute,
				// The default must resume the responder; a fresh executor is a failure.
				Executor: reflectionTaskExecutorFunc(func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error) {
					t.Fatal("fresh reflection session")
					return nil, nil
				}),
				SessionExecutor: func(id string) (ReflectionTaskExecutor, error) {
					if id != scenario {
						t.Fatalf("session incident=%s", id)
					}
					return reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
						calls++
						request := &contracts.ReflectionRequest{}
						if err := task.Request.UnmarshalTo(request); err != nil {
							t.Fatal(err)
						}
						if request.SessionMode != "resume" || request.GetClosure().GetResult().GetResponderSessionId() != closure.Result.ResponderSessionID {
							t.Fatalf("lost responder session: %v", request)
						}
						if len(request.Outcome.ConfirmedRootCauses) != 0 || len(request.Outcome.RepairActions) != 0 || len(request.CauseWork) != 0 {
							t.Fatalf("unverified cause given learning authority: %v", request)
						}
						if scenario == "failed-social-repeat" {
							if len(request.UnassignedWork) != 2 {
								t.Fatalf("missing per-cause evidence gaps: %v", request.UnassignedWork)
							}
							for _, item := range request.UnassignedWork {
								if item.Reason != "evidence_gap" || len(item.Resources) != 1 {
									t.Fatalf("untyped evidence gap: %v", item)
								}
							}
						}
						if !task.Deadline.AsTime().After(closure.Request.ResponseDeadline) {
							t.Fatal("reflection inherited expired response budget")
						}
						response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "no independently verified repaired cause"})
						return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
					}), nil
				},
			}
			receipt, err := native.ProcessClosure(ctx, closure)
			if err != nil {
				t.Fatal(err)
			}
			again, err := native.ProcessClosure(ctx, closure)
			if err != nil || again != receipt || calls != 1 {
				t.Fatalf("reflection calls=%d receipt=%v err=%v", calls, again, err)
			}
			ledger, err := native.readLedger(scenario)
			if err != nil {
				t.Fatal(err)
			}
			if !ledger.RequiresReflection || !ledger.ReflectionProcessed || ledger.ReflectionDecision.GetLearningDecision() != "no_change" {
				t.Fatalf("missing durable reflection: %v", ledger)
			}
			if scenario == "failed-social-repeat" && (ledger.OutcomeDocument.Classification != "failed" || len(ledger.OutcomeDocument.ConfirmedRootCauses) != 2) {
				t.Fatal("reflection rewrote authoritative failed evidence")
			}
		})
	}
}

func TestReflectionFailedClosureRestartBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	proposals, ledgers := filepath.Join(t.TempDir(), "proposals"), t.TempDir()
	makeBroker := func() NativeIncidentBroker {
		repo, err := broker.New(root, proposals, nil)
		if err != nil {
			t.Fatal(err)
		}
		return NativeIncidentBroker{Repository: repo, LedgerRoot: ledgers, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
	}
	first := makeBroker()
	workspace, err := first.PrepareIncident(ctx, "restart-failed")
	if err != nil {
		t.Fatal(err)
	}
	closure := socialRepeatClosure(workspace)
	if _, err := first.ProcessClosure(ctx, closure); err == nil {
		t.Fatal("closure acknowledged before required reflection dispatch")
	}
	ledger, err := first.readLedger(workspace.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.OutcomeCommit == "" || ledger.Receipt != nil || !ledger.RequiresReflection {
		t.Fatalf("missing durable pre-dispatch closure: %v", ledger)
	}
	restarted := makeBroker()
	calls := 0
	restarted.Reflector = ReflectionWorkerFunc(func(_ context.Context, c IncidentClosure, _ IncidentWorkspace, o *contracts.OutcomeRecord, _, _ string, feedback ReflectionFeedback) (ReflectionDecision, error) {
		calls++
		if c.Result.ResponderSessionID != closure.Result.ResponderSessionID || feedback.DispatchAt.IsZero() || o.Classification != "failed" {
			t.Fatal("restart lost closure/session/dispatch evidence")
		}
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "per-cause proof unavailable"}, nil
	})
	receipt, err := restarted.ProcessClosure(ctx, closure)
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.ProcessClosure(ctx, closure)
	if err != nil || again != receipt || calls != 1 {
		t.Fatalf("restart reflection calls=%d err=%v", calls, err)
	}
}

func TestReflectionFailedTaskRestartResumesSameSessionAndIdentity(t *testing.T) {
	root := nativeRepository(t)
	workspace := IncidentWorkspace{IncidentID: "retry-failed", Worktree: root, BaseCommit: nativeGit(t, root, "rev-parse", "HEAD")}
	closure := socialRepeatClosure(workspace)
	store := &lifecycleio.TaskStore{Root: t.TempDir()}
	var frozen *contracts.AgentTask
	calls := 0
	worker := RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute, TaskStore: store,
		Executor: reflectionTaskExecutorFunc(func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error) {
			t.Fatal("fresh executor used")
			return nil, nil
		}),
		SessionExecutor: func(string) (ReflectionTaskExecutor, error) {
			return reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
				calls++
				if frozen == nil {
					frozen = proto.Clone(task).(*contracts.AgentTask)
					return nil, errors.New("transport interrupted")
				}
				if !proto.Equal(frozen, task) {
					t.Fatal("restart changed task identity, payload or deadline")
				}
				response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "missing proof"})
				return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
			}), nil
		},
	}
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: workspace.IncidentID, Classification: "failed"}
	feedback := ReflectionFeedback{DispatchAt: time.Now().UTC()}
	if _, err := worker.Reflect(context.Background(), closure, workspace, outcome, "", "outcome", feedback); err == nil {
		t.Fatal("expected transport interruption")
	}
	restarted := worker
	restarted.Timeout = time.Hour
	feedback.DispatchAt = feedback.DispatchAt.Add(time.Hour)
	if _, err := restarted.Reflect(context.Background(), closure, workspace, outcome, "", "outcome", feedback); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("transport calls=%d", calls)
	}
}
