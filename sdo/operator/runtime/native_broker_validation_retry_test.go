package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// Exercise both retry boundaries: an inspection retry in the same validator
// Job, and recovery from a validation transport error after broker restart.
func TestNativeBrokerRetriesValidationWithoutRedispatchingReflection(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			var reflectionTask *contracts.AgentTask
			var failedJob string
			inspections, jobs := 0, 0
			client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, kuberuntime.Object, error) {
				job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				jobs++
				finished := metav1.Now()
				job.Status = batchv1.JobStatus{Succeeded: 1, CompletionTime: &finished}
				if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
					t.Fatal(err)
				}
				return true, job, nil
			})
			client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, kuberuntime.Object, error) {
				name := action.(ktesting.GetAction).GetName()
				if _, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), "sdo", name); err != nil {
					return false, nil, nil
				}
				if reflectionTask != nil && !strings.HasSuffix(name, "-allow") && !strings.HasSuffix(name, "-deny") {
					inspections++
					if inspections == 1 {
						failedJob = name
						return true, nil, apierrors.NewServiceUnavailable("rpc error: code = Unavailable desc = etcd connection timed out")
					}
					if name != failedJob {
						t.Fatal("transient inspection error replaced the validator Job")
					}
				}
				return false, nil, nil
			})
			var launcher broker.KubernetesValidationJobClient
			validationCalls := 0
			h := newRejectionHarnessValidator(t, "inc-validation-retry", nil, func(ctx context.Context, root string, paths []string) error {
				if reflectionTask != nil {
					validationCalls++
					if restart && validationCalls == 1 {
						return fmt.Errorf("inspect Job sdo-validator-test: %w", apierrors.NewServiceUnavailable("etcd connection timed out"))
					}
				}
				return broker.JobValidator(launcher)(ctx, root, paths)
			})
			workspaceRoot := filepath.Dir(filepath.Dir(filepath.Dir(h.workspace.Worktree)))
			launcher = broker.KubernetesValidationJobClient{
				Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository",
				RepositoryMountPath: workspaceRoot, SchemaIdentity: "schema",
				PollInterval: time.Millisecond, Timeout: time.Second,
			}
			dispatches := 0
			h.native.Reflector = RegisteredReflectionWorker{
				AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.NewJSONStore(t.TempDir()), Timeout: time.Minute,
				Executor: reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
					dispatches++
					if reflectionTask != nil && !proto.Equal(reflectionTask, task) {
						return nil, errors.New("existing agent request ConfigMap has different payload")
					}
					reflectionTask = proto.Clone(task).(*contracts.AgentTask)
					if _, err := writeGoodPlaybook(IncidentWorkspace{Worktree: task.Workspace.CheckoutPath}); err != nil {
						return nil, err
					}
					// Prose alone skips the validator Job; a library edit exercises it.
					library := filepath.Join(task.Workspace.CheckoutPath, ".sdo", "lib", "base")
					if err := os.MkdirAll(library, 0o755); err != nil {
						return nil, err
					}
					if err := os.WriteFile(filepath.Join(library, "helper.go"), []byte("package base\n"), 0o644); err != nil {
						return nil, err
					}
					response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{"base"}})
					return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
				}),
			}
			if restart {
				if _, err := h.native.ProcessClosure(context.Background(), h.closure); err == nil || !strings.Contains(err.Error(), "etcd connection timed out") {
					t.Fatalf("expected interrupted validation: %v", err)
				}
				// A fresh coordinator must recover the durable decision and checkout.
				repository, err := broker.New(h.target, workspaceRoot, func(ctx context.Context, root string, paths []string) error {
					return broker.JobValidator(launcher)(ctx, root, paths)
				})
				if err != nil {
					t.Fatal(err)
				}
				h.native.Repository = repository
			}
			receipt, err := h.native.ProcessClosure(context.Background(), h.closure)
			if err != nil {
				t.Fatalf("closure did not recover: %v", err)
			}
			if receipt.ReflectionCommit == "" || receipt.OutcomeCommit == "" || dispatches != 1 || inspections != 2 || jobs != 6 {
				t.Fatalf("receipt=%+v dispatches=%d inspections=%d jobs=%d", receipt, dispatches, inspections, jobs)
			}
			ledger, err := h.native.readLedger(h.workspace.IncidentID)
			if err != nil || len(ledger.GetReflectionRejections()) != 0 || !ledger.GetReflectionProcessed() {
				t.Fatalf("transient API error became a rejection: ledger=%v err=%v", ledger, err)
			}
			if got := nativeGit(t, h.target, "show", receipt.ReflectionCommit+":.sdo/playbooks/base/README.md"); !strings.Contains(got, "carefully") {
				t.Fatal("completed reflection edit was lost")
			}
		})
	}
}
