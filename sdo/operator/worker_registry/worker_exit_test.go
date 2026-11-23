package worker_registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// workerExit plays a worker that publishes its result and exits, while the Job controller has not yet reported
// the Job Complete (it lags the pod by seconds on a virtual cluster). podPhase is the phase the worker's pod reaches.
func workerExit(t *testing.T, mount string, podPhase corev1.PodPhase) (*fake.Clientset, *contracts.AgentTask) {
	t.Helper()
	registry := registrytest.BuiltIn(t)
	request, err := anypb.New(&contracts.HealthJudgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	agentID, err := registry.AgentFor(&contracts.HealthJudgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	task := &contracts.AgentTask{
		SchemaVersion: worker_registry.SchemaVersion, TaskId: "judge-1", AgentId: agentID,
		TaskType: registrytest.TaskType(t, agentID, &contracts.HealthJudgeRequest{}), Request: request,
		Workspace: &contracts.ProposalWorkspace{CheckoutPath: mount + "/checkouts/judge-1", BaseCommit: "abc"},
	}
	response, err := anypb.New(&contracts.HealthJudgeResult{})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := wire.Canonical(&contracts.AgentResult{SchemaVersion: worker_registry.SchemaVersion, TaskId: task.GetTaskId(), Status: "completed", Response: response})
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-pod", Namespace: job.Namespace, Labels: map[string]string{"batch.kubernetes.io/job-name": job.Name, "job-name": job.Name}},
			Status:     corev1.PodStatus{Phase: podPhase},
		}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("pods"), pod, job.Namespace); err != nil {
			t.Error(err)
		}
		result := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: job.Name + "-result", Namespace: job.Namespace}, Data: map[string]string{worker_registry.ResultDataKey: string(payload)}}
		if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), result, job.Namespace); err != nil {
			t.Error(err)
		}
		return false, nil, nil
	})
	return client, task
}

func executor(t *testing.T, client *fake.Clientset, mount string) worker_registry.KubernetesExecutor {
	return worker_registry.KubernetesExecutor{
		Client: client, Namespace: "sdo", Registry: registrytest.BuiltIn(t), RepositoryPVC: "repo",
		RepositoryMountPath: mount, PollInterval: 10 * time.Millisecond,
	}
}

// The worker's exit is the event that ends a task: once its pod has succeeded with the result published, the
// controller has the result, without waiting for the Job controller to derive the Job's Complete condition.
func TestAResultIsTakenWhenTheWorkerPodSucceededBeforeTheJobReportsComplete(t *testing.T) {
	mount := t.TempDir()
	client, task := workerExit(t, mount, corev1.PodSucceeded)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := executor(t, client, mount).Execute(ctx, task)
	if err != nil {
		t.Fatalf("Execute = %v; want the published result once the worker pod succeeded", err)
	}
	if result.GetTaskId() != task.GetTaskId() || result.GetStatus() != "completed" {
		t.Fatalf("result = %v", result)
	}
}

// A published result alone does not end the task while the worker is still running: it publishes before it
// exits and may still write.
func TestAResultIsHeldWhileTheWorkerPodStillRuns(t *testing.T) {
	mount := t.TempDir()
	client, task := workerExit(t, mount, corev1.PodRunning)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	result, err := executor(t, client, mount).Execute(ctx, task)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute = %v, %v; want it to keep waiting while the worker pod runs", result, err)
	}
}
