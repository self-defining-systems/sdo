package worker_registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func TestKubernetesExecutorNeverRecreatesWorkerWithUnsettledResult(t *testing.T) {
	for _, scenario := range []string{"orphan-pod", "pod-api-error", "invalid-result", "late-result", "job-deadline-exceeded"} {
		t.Run(scenario, func(t *testing.T) {
			task := reflectionTestTask(t)
			task.Deadline = timestamppb.New(time.Now().Add(time.Minute))
			if scenario == "late-result" || scenario == "job-deadline-exceeded" {
				task.Deadline = timestamppb.New(time.Now().Add(-time.Minute))
			}
			name := JobName(task.TaskId)
			payload, err := wire.Canonical(task)
			if err != nil {
				t.Fatal(err)
			}
			response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "no_change", NoChangeReason: "covered"})
			if err != nil {
				t.Fatal(err)
			}
			result := &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}
			if scenario == "invalid-result" {
				result.TaskId = "another-task"
			}
			data, err := wire.Canonical(result)
			if err != nil {
				t.Fatal(err)
			}
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-request", Namespace: "demo"}, Data: map[string]string{TaskDataKey: string(payload)}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-result", Namespace: "demo", CreationTimestamp: metav1.Now()}, Data: map[string]string{ResultDataKey: string(data)}})
			if scenario == "job-deadline-exceeded" {
				if _, err := client.BatchV1().Jobs("demo").Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				client.ClearActions()
			}
			if scenario == "orphan-pod" {
				if _, err := client.CoreV1().Pods("demo").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orphan", Labels: map[string]string{"job-name": name}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "pod-api-error" {
				client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("pod API unavailable")
				})
			}
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace", PollInterval: time.Millisecond}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			got, err := executor.Execute(ctx, task)
			if scenario == "job-deadline-exceeded" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("recovered Job lost typed deadline failure: %v", err)
			}
			if err == nil || got != nil {
				t.Fatalf("unsettled or mismatched result was accepted: %v %v", got, err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() == "create" && action.GetResource().Resource == "jobs" {
					t.Fatal("result recovery recreated a worker before resolving prior execution")
				}
			}
		})
	}
}
