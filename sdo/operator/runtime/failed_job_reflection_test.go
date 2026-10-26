package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk/sdktest"
)

// The provider checkpoint reaches the real incident state store before a Job
// fails without writing an AgentResult. A replacement dispatcher must recover
// it, and the controller/broker must preserve it through failed closure.
func TestReflectionFailedJobRecoversCheckpointOrRecordsNoSession(t *testing.T) {
	for _, saved := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-session", true: "saved-session"}[saved], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			root := nativeRepository(t)
			repo, err := broker.New(root, filepath.Join(t.TempDir(), "proposals"), nil)
			if err != nil {
				t.Fatal(err)
			}
			n := NativeIncidentBroker{Repository: repo, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
			workspace, err := n.PrepareIncident(ctx, "failed-job")
			if err != nil {
				t.Fatal(err)
			}
			closure := socialRepeatClosure(workspace)
			closure.Result = nil
			request := closure.Request
			request.ResponseDeadline = time.Now().Add(time.Minute)
			closure.Request = request
			client := fake.NewSimpleClientset()
			d := KubernetesJobDispatcher{Client: client, Namespace: "demo", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), ServiceAccount: "worker", RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(root), PollInterval: time.Millisecond}
			d.LoopFactory = func(r IncidentRequest) *ResponderLoop {
				return NewResponderLoop(r, nil, staticProvider{snapshot: sdktest.Snapshot{}}, nil, PlanExecutor{}, 2)
			}
			// Proposals live on the same mounted volume in production.
			d.RepositoryMountPath = filepath.Dir(filepath.Dir(workspace.Worktree))
			if saved {
				loop := d.LoopFactory(request)
				if err := d.restoreToolState(ctx, request, loop); err != nil {
					t.Fatal(err)
				}
				opened := loop.Handle(ctx, ToolRequest{Command: "session-open"})
				if opened.Error != "" {
					t.Fatal(opened.Error)
				}
				checkpoint := loop.Handle(ctx, ToolRequest{Command: "session-checkpoint", Checkpoint: &contracts.MitigationSessionCheckpoint{Provider: "codex", ProviderSessionId: "provider-before-oom", CheckpointDigest: "checkpoint"}})
				if checkpoint.Error != "" {
					t.Fatal(checkpoint.Error)
				}
			}
			// New loop instance loads the saved checkpoint, as after controller restart.
			done := make(chan dispatchCompletion, 1)
			go func() {
				r, err := d.Dispatch(ctx, request)
				if err != nil {
					t.Logf("dispatch returned: %v", err)
				}
				done <- dispatchCompletion{incidentID: request.IncidentID, result: r, err: err}
			}()
			job := awaitJob(t, client, IncidentJobName(request.IncidentID))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: "True"}}
			if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			completion := <-done
			if completion.err == nil {
				t.Fatal("failed Job succeeded")
			}
			now := time.Now().UTC()
			c, _ := newDispatchRetryController(t, DispatchRetryPolicy{}, &now)
			c.currentIncidentRequest = &request
			c.handleDispatchCompletion(completion, now)
			closure.Result = c.currentIncidentResult
			calls := 0
			n.Reflector = RegisteredReflectionWorker{AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: time.Minute,
				Executor: reflectionTaskExecutorFunc(func(context.Context, *contracts.AgentTask) (*contracts.AgentResult, error) {
					t.Fatal("fresh session started")
					return nil, nil
				}),
				SessionExecutor: func(string) (ReflectionTaskExecutor, error) {
					calls++
					if closure.Result == nil || closure.Result.ResponderSessionID != "provider-before-oom" {
						t.Fatal("lost saved session")
					}
					return reflectionTaskExecutorFunc(reflectionNoChangeResult), nil
				},
			}
			receipt, err := closeAndReflect(t, ctx, n, closure)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := closeAndReflect(t, ctx, n, closure); err != nil {
				t.Fatal(err)
			}
			ledger, err := n.readLedger(request.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			if saved {
				if calls != 1 || !ledger.ReflectionProcessed {
					t.Fatalf("saved session reflection calls=%d ledger=%v", calls, ledger)
				}
			} else {
				if calls != 0 || ledger.ReflectionDecision.GetNoChangeReason() != "no_session" {
					t.Fatalf("missing visible no_session skip: %v", ledger)
				}
				outcomes, err := memory.OpenReader(root).List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if outcomes[0].Record.GetReflectionSkipReason() != "no_session" {
					t.Fatalf("accepted outcome omitted typed skip: %v", outcomes[0].Record)
				}
			}
			if receipt.ReflectionCommit != "" {
				t.Fatal("gap-only reflection published executable memory")
			}
		})
	}
}

func reflectionNoChangeResult(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "independent cause evidence unavailable"})
	return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
}
