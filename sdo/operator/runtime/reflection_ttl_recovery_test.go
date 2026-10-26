package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

func TestReflectionCompletedResultSurvivesJobTTLAndBrokerRestart(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(map[bool]string{false: "within-deadline", true: "after-deadline"}[expired], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				root := nativeRepository(t)
				proposals := filepath.Join(t.TempDir(), "proposals")
				ledgers := t.TempDir()
				repo, err := broker.New(root, proposals, nil)
				if err != nil {
					t.Fatal(err)
				}
				native := NativeIncidentBroker{Repository: repo, LedgerRoot: ledgers, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
				workspace, err := native.PrepareIncident(ctx, "ttl-recovery")
				if err != nil {
					t.Fatal(err)
				}
				closure := socialRepeatClosure(workspace)
				client := fake.NewSimpleClientset()
				registry := testIncidentRegistry(t, "reflector:test")
				agent, err := registry.AgentFor(&contracts.ReflectionRequest{})
				if err != nil {
					t.Fatal(err)
				}
				created := make(chan struct{}, 1)
				executor := worker_registry.KubernetesExecutor{OnPhase: func(_ context.Context, phase string) error {
					if phase == "reflection_job_created" {
						created <- struct{}{}
					}
					return nil
				}, Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(filepath.Dir(workspace.Worktree)), PollInterval: time.Millisecond}
				store := &lifecycleio.TaskStore{Root: t.TempDir()}
				worker := RegisteredReflectionWorker{AgentID: agent, TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: 300 * time.Millisecond, TaskStore: store, Executor: executor, SessionExecutor: func(string) (ReflectionTaskExecutor, error) { return executor, nil }}
				native.Reflector = ReflectionWorkerFunc(func(ctx context.Context, c IncidentClosure, w IncidentWorkspace, o *contracts.OutcomeRecord, p, oc string, f ReflectionFeedback) (ReflectionDecision, error) {
					decision, err := worker.Reflect(ctx, c, w, o, p, oc, f)
					if err != nil {
						return decision, err
					}
					return ReflectionDecision{}, errors.New("controller stopped after completed result")
				})
				done := make(chan error, 1)
				goUntilCleanup(t, cancel, func() { _, err := closeAndReflect(t, ctx, native, closure); done <- err })
				select {
				case <-created:
				case err := <-done:
					t.Fatalf("reflection exited before Job creation: %v", err)
				case <-ctx.Done():
					t.Fatal("real reflection executor did not create Job")
				}
				jobs, err := client.BatchV1().Jobs("demo").List(ctx, metav1.ListOptions{})
				if err != nil || len(jobs.Items) != 1 {
					t.Fatalf("created reflection Job is missing: %v %v", jobs, err)
				}
				jobName := jobs.Items[0].Name
				request, err := client.CoreV1().ConfigMaps("demo").Get(ctx, jobName+"-request", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				task := new(contracts.AgentTask)
				if err := wire.Unmarshal([]byte(request.Data[worker_registry.TaskDataKey]), task); err != nil {
					t.Fatal(err)
				}
				response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "independent cause proof unavailable"})
				if err != nil {
					t.Fatal(err)
				}
				result := &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
				payload, err := wire.Canonical(result)
				if err != nil {
					t.Fatal(err)
				}
				now := metav1.Now()
				if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: jobName + "-result", CreationTimestamp: now}, Data: map[string]string{worker_registry.ResultDataKey: string(payload)}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				job, err := client.BatchV1().Jobs("demo").Get(ctx, jobName, metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				job.Status.Succeeded = 1
				job.Status.CompletionTime = &now
				if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				if err := <-done; err == nil || !strings.Contains(err.Error(), "controller stopped after completed result") {
					t.Fatalf("did not reach crash boundary: %v", err)
				}
				// Kubernetes TTL deletes the completed Job; the durable result remains.
				if err := client.BatchV1().Jobs("demo").Delete(ctx, jobName, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
				if expired {
					if wait := time.Until(task.Deadline.AsTime()) + 5*time.Millisecond; wait > 0 {
						<-time.After(wait)
					}
				}
				repo, err = broker.New(root, proposals, nil)
				if err != nil {
					t.Fatal(err)
				}
				native = NativeIncidentBroker{Repository: repo, LedgerRoot: ledgers, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test", Reflector: worker}
				if _, err := closeAndReflect(t, ctx, native, closure); err != nil {
					t.Fatal(err)
				}
				ledger, err := native.readLedger(workspace.IncidentID)
				if err != nil {
					t.Fatal(err)
				}
				if !ledger.ReflectionProcessed || ledger.ReflectionDecision.GetLearningDecision() != "no_change" || ledger.ReflectionDecision.GetFailureReason() != "" || len(ledger.ReflectionResults) != 1 {
					t.Fatalf("durable completed result lost after TTL: %v", ledger)
				}
				creates := 0
				for _, action := range client.Actions() {
					if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
						creates++
					}
				}
				if creates != 1 {
					t.Fatalf("completed reflection executed again after TTL: Job creates=%d", creates)
				}
			})
		})
	}
}
