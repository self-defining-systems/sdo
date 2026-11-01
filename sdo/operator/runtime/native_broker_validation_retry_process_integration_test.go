//go:build integration

package runtime

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"path/filepath"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"strings"
	"testing"
	"time"
)

// Infrastructure failures preserve the completed reflection across broker restart.
func TestNativeBrokerRetriesValidationWithoutRedispatchingReflection(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			var reflectionTask *contracts.AgentTask
			validationCalls := 0
			h := newRejectionHarnessValidator(t, "inc-validation-retry", nil, func(ctx context.Context, root string, paths []string) error {
				if reflectionTask != nil {
					validationCalls++
					if restart && validationCalls == 1 {
						return fmt.Errorf("validation subprocess: %w", apierrors.NewServiceUnavailable("etcd connection timed out"))
					}
				}
				return nil
			})
			workspaceRoot := filepath.Dir(filepath.Dir(filepath.Dir(h.workspace.Worktree)))
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
					return nil
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
			if receipt.ReflectionCommit == "" || receipt.OutcomeCommit == "" || dispatches != 1 {
				t.Fatalf("receipt=%+v dispatches=%d", receipt, dispatches)
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
