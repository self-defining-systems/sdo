package worker_registry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/contracts/wire"
)

func TestKubernetesExecutorVanishedJobIsTerminalAcrossRestart(t *testing.T) {
	for _, scenario := range []string{"reflection", "conflicting-request-reflection", "responder"} {
		t.Run(scenario, func(t *testing.T) {
			task := reflectionTestTask(t)
			client := fake.NewSimpleClientset()
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: t.TempDir(), PollInterval: time.Millisecond}
			task.Workspace.CheckoutPath = executor.RepositoryMountPath + "/proposals/reflection-1"
			name := JobName(task.TaskId)
			if scenario == "conflicting-request-reflection" {
				if err := executor.ensureRequest(context.Background(), name+"-request", `{"different":"request"}`); err != nil {
					t.Fatal(err)
				}
				name = TaskJobName(task)
			}
			if scenario == "responder" {
				_, executor, task = standbyFixture(t)
				client = executor.Client.(*fake.Clientset)
				name = JobName(task.TaskId)
			}
			executor.OnPhase = func(ctx context.Context, phase string) error {
				if phase == "reflection_job_created" {
					return client.BatchV1().Jobs(executor.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := executor.Execute(ctx, task)
			assertVanished := func(err error) {
				t.Helper()
				var terminal interface{ NonRetryable() bool }
				var vanished *JobVanishedError
				if !errors.As(err, &vanished) || vanished.TaskID != task.TaskId || vanished.JobName != name {
					t.Fatalf("lost typed vanished identity: %v", err)
				}
				if !errors.As(err, &terminal) || !terminal.NonRetryable() || !strings.Contains(err.Error(), "worker Job vanished") || !strings.Contains(err.Error(), name) {
					t.Fatalf("vanished worker did not fail terminal with identity: %v", err)
				}
			}
			assertVanished(err)
			// Fresh transport, same durable API evidence: recovery is read-only and no
			// new execution can appear under the frozen task identity.
			executor.OnPhase = nil
			client.ClearActions()
			_, recovered, err := executor.RecoverCompleted(context.Background(), task)
			if recovered {
				t.Fatal("missing result was recovered")
			}
			assertVanished(err)
			for _, a := range client.Actions() {
				if a.GetVerb() != "get" && a.GetVerb() != "list" {
					t.Fatalf("recovery mutated API: %v", a)
				}
			}
			_, err = executor.Execute(context.Background(), task)
			assertVanished(err)
			for _, a := range client.Actions() {
				if a.GetVerb() == "create" && a.GetResource().Resource == "jobs" {
					t.Fatal("restart recreated vanished worker")
				}
				if a.GetResource().Resource == "secrets" {
					t.Fatal("disappearance read Secrets")
				}
			}
		})
	}
}

func TestKubernetesExecutorMissingJobBeforeDispatchIsNotVanished(t *testing.T) {
	task := reflectionTestTask(t)
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t)}
	// Preparation may precede Job creation across a controller restart. Neither
	// an absent request nor a prepared request is evidence of dispatched work.
	for _, prepared := range []bool{false, true} {
		if prepared {
			payload, err := wire.Canonical(task)
			if err != nil {
				t.Fatal(err)
			}
			if err := executor.ensureRequest(context.Background(), JobName(task.TaskId)+"-request", string(payload)); err != nil {
				t.Fatal(err)
			}
		}
		result, recovered, err := executor.RecoverCompleted(context.Background(), task)
		if result != nil || recovered || err != nil {
			t.Fatalf("undispatched worker declared vanished: %v %v %v", result, recovered, err)
		}
	}
}

func TestKubernetesExecutorMissingJobAPIErrorRemainsRetryable(t *testing.T) {
	task := reflectionTestTask(t)
	client := fake.NewSimpleClientset()
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t)}
	unavailable := errors.New("Job API unavailable")
	client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, unavailable })
	_, recovered, err := executor.RecoverCompleted(context.Background(), task)
	var terminal interface{ NonRetryable() bool }
	if recovered || !errors.Is(err, unavailable) || errors.As(err, &terminal) {
		t.Fatalf("API uncertainty became permanent disappearance: %v %v", recovered, err)
	}
}
