package worker_registry

import (
	"context"
	"errors"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func TestKubernetesExecutorFailedJobCarriesTerminalEvidence(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "worker-failed", true: "deadline"}[deadline], func(t *testing.T) {
			task := reflectionTestTask(t)
			task.Deadline = timestamppb.New(time.Now().Add(time.Minute))
			name := JobName(task.TaskId)
			reason := "BackoffLimitExceeded"
			if deadline {
				reason = "DeadlineExceeded"
			}
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo", UID: "worker-job"}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: reason, Message: "worker exhausted retries"}}}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-worker", Namespace: "demo", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name, UID: job.UID}}}, Status: corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137, Message: "memory limit exceeded"}}}}}}
			// A label collision must not contaminate the task's diagnostics.
			foreign := pod.DeepCopy()
			foreign.Name = "foreign"
			foreign.OwnerReferences[0].UID = "another-job"
			foreign.Labels = map[string]string{"batch.kubernetes.io/job-name": name}
			foreign.Status.ContainerStatuses[0].State.Terminated.Message = "foreign evidence"
			client := fake.NewSimpleClientset(job, pod, foreign)
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace", PollInterval: time.Millisecond}
			_, err := executor.Execute(context.Background(), task)
			if deadline {
				var timeout *TaskTimeoutError
				if !errors.As(err, &timeout) || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline lost typed branch: %v", err)
				}
				return
			}
			var failed *JobFailedError
			if !errors.As(err, &failed) {
				t.Fatalf("missing typed failure: %v", err)
			}
			var terminal interface{ NonRetryable() bool }
			if !errors.As(err, &terminal) || !terminal.NonRetryable() {
				t.Errorf("terminal Job failure is retryable: %v", err)
			}
			for _, want := range []string{name, "Failed", "BackoffLimitExceeded", "worker exhausted retries", "failed-worker", "agent", "OOMKilled", "137", "memory limit exceeded"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("failure missing %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), "foreign evidence") {
				t.Errorf("foreign pod included: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetResource().Resource == "secrets" {
					t.Fatal("failure diagnostics read Secrets")
				}
			}
		})
	}
}

func TestKubernetesExecutorFailedResultRecoveryRetainsEvidence(t *testing.T) {
	task := reflectionTestTask(t)
	name := JobName(task.TaskId)
	payload, _ := wire.Canonical(task)
	response, _ := anypb.New(&contracts.ReflectionResult{LearningDecision: "failed"})
	result, _ := wire.Canonical(&contracts.AgentResult{SchemaVersion: SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "failed", ErrorSummary: "worker failed", Response: response})
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo", UID: "worker"}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "worker exited"}}}}
	client := fake.NewSimpleClientset(job, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-request", Namespace: "demo"}, Data: map[string]string{TaskDataKey: string(payload)}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-result", Namespace: "demo"}, Data: map[string]string{ResultDataKey: string(result)}})
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("pod API unavailable")
	})
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t)}
	_, recovered, err := executor.RecoverCompleted(context.Background(), task)
	var failed *JobFailedError
	if recovered || !errors.As(err, &failed) || !strings.Contains(err.Error(), "BackoffLimitExceeded") || !strings.Contains(err.Error(), "pod API unavailable") {
		t.Fatalf("failed result lost terminal evidence: recovered=%v err=%v", recovered, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("recovery mutated cluster: %v", action)
		}
	}
}

func TestKubernetesExecutorFailureDiagnosticsBoundStatusText(t *testing.T) {
	task := reflectionTestTask(t)
	name := JobName(task.TaskId)
	message := strings.Repeat("界", 4096)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "worker", Namespace: "demo"}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: message}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-init", Namespace: "demo", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name, UID: job.UID}}}, Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 42, Message: message}}}}}}
	client := fake.NewSimpleClientset(job, pod)
	executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace"}
	_, err := executor.Execute(context.Background(), task)
	var failed *JobFailedError
	if !errors.As(err, &failed) || len(err.Error()) > 4096 || !strings.Contains(err.Error(), "exit_code=42") || !strings.Contains(err.Error(), "…") {
		t.Fatalf("status text escaped diagnostic budget: %v", err)
	}
	if len(failed.Conditions) != 1 || len(failed.Containers) != 1 || failed.Containers[0].ExitCode != 42 || failed.Containers[0].ContainerName != "init" {
		t.Fatalf("structured termination evidence lost: %+v", failed)
	}
}

// The cause of a crash is the last thing the worker logged, so a bounded message keeps its end.
func TestKubernetesExecutorFailureDiagnosticsBoundTheCuratedLineAndRepairUTF8(t *testing.T) {
	task := reflectionTestTask(t)
	name := JobName(task.TaskId)
	message := "RuntimeError: " + strings.Repeat("x", 2000) + "\xe2\x82"
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "worker", Namespace: "demo"}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-agent", Namespace: "demo", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name, UID: job.UID}}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, Message: message}}}}}}
	executor := KubernetesExecutor{Client: fake.NewSimpleClientset(job, pod), Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace"}
	_, err := executor.Execute(context.Background(), task)
	var failed *JobFailedError
	if !errors.As(err, &failed) || len(failed.Containers) != 1 {
		t.Fatalf("want a JobFailedError with the container: %v", err)
	}
	got := failed.Containers[0].Message
	if !utf8.ValidString(got) || len([]rune(got)) > 513 || !strings.HasPrefix(got, "RuntimeError: ") {
		t.Fatalf("termination line not bounded and valid UTF-8: %d runes", len([]rune(got)))
	}
}
