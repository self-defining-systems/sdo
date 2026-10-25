package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/worker_registry"
)

// A real registered task and Kubernetes executor leave the fake Job Pending.
// The frozen deadline must terminate the broker obligation, including a crash
// after Job creation but before its first terminal reflection decision.
func TestReflectionPendingJobDeadlineIsFrozenAndTerminal(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending-timeout", true: "restart-after-deadline"}[restart], func(t *testing.T) {
			root := nativeRepository(t)
			proposals := filepath.Join(t.TempDir(), "proposals")
			ledgers := t.TempDir()
			repo, err := broker.New(root, proposals, nil)
			if err != nil {
				t.Fatal(err)
			}
			native := NativeIncidentBroker{Repository: repo, LedgerRoot: ledgers, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
			workspace, err := native.PrepareIncident(context.Background(), "pending-reflection")
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
			parent, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: registry, RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(filepath.Dir(workspace.Worktree)), PollInterval: time.Millisecond}
			if restart {
				executor.OnPhase = func(_ context.Context, phase string) error {
					if phase == "reflection_job_created" {
						time.AfterFunc(10*time.Millisecond, cancel)
					}
					return nil
				}
			}
			store := &lifecycleio.TaskStore{Root: t.TempDir()}
			worker := RegisteredReflectionWorker{AgentID: agent, TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Timeout: 120 * time.Millisecond, TaskStore: store, Executor: executor, SessionExecutor: func(string) (ReflectionTaskExecutor, error) {
				return reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
					deadline, ok := ctx.Deadline()
					if !ok || !deadline.Equal(task.GetDeadline().AsTime()) {
						t.Errorf("registered worker did not bind wait context to frozen task deadline: %s", deadline)
					}
					return executor.Execute(ctx, task)
				}), nil
			}}
			native.Reflector = worker
			_, firstErr := native.ProcessClosure(parent, closure)
			if restart {
				if !errors.Is(firstErr, context.Canceled) {
					t.Fatalf("expected injected leadership interruption, got %v", firstErr)
				}
			} else if firstErr != nil {
				t.Errorf("frozen reflection deadline did not produce terminal outcome: %v", firstErr)
			}
			ledger, err := native.readLedger(workspace.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			task, found, err := store.Load(ledger.GetReflectionWorkspace().GetProposalId())
			if err != nil || !found {
				t.Fatalf("frozen task missing: %v", err)
			}
			job, err := client.BatchV1().Jobs("demo").Get(context.Background(), worker_registry.TaskJobName(task), metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 1 {
				t.Errorf("Pending Job is not bounded by frozen deadline: %v", job.Spec.ActiveDeadlineSeconds)
			}
			if restart {
				if wait := time.Until(task.Deadline.AsTime()) + 5*time.Millisecond; wait > 0 {
					<-time.After(wait)
				}
				repo, err = broker.New(root, proposals, nil)
				if err != nil {
					t.Fatal(err)
				}
				native = NativeIncidentBroker{Repository: repo, LedgerRoot: ledgers, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "test", ResponderModel: "test"}
				worker.Timeout = time.Hour
				native.Reflector = worker
				ctx, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if _, err := native.ProcessClosure(ctx, closure); err != nil {
					t.Fatalf("expired frozen task was not recovered as timeout: %v", err)
				}
			} else if firstErr != nil {
				return
			}
			ledger, err = native.readLedger(workspace.IncidentID)
			if err != nil {
				t.Fatal(err)
			}
			if !ledger.ReflectionProcessed || ledger.ReflectionDecision.GetLearningDecision() != "failed" || ledger.ReflectionDecision.GetFailureReason() != "timeout" || ledger.ReflectionDecision.GetError() == "" || len(ledger.ReflectionResults) != 1 || len(ledger.ReflectionRejections) != 0 || ledger.ReflectionCommit != "" {
				t.Fatalf("timeout not terminal and typed: %v", ledger)
			}
			frozen, found, err := store.Load(task.TaskId)
			if err != nil || !found || !frozen.Deadline.AsTime().Equal(task.Deadline.AsTime()) {
				t.Fatalf("restart extended deadline: %v", err)
			}
			if _, err := native.ProcessClosure(context.Background(), closure); err != nil {
				t.Fatal(err)
			}
			creates := 0
			for _, a := range client.Actions() {
				if a.GetVerb() == "create" && a.GetResource().Resource == "jobs" {
					creates++
				}
			}
			if creates != 1 {
				t.Fatalf("timeout created %d worker Jobs for one logical reflection", creates)
			}
		})
	}
}
