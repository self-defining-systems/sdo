package worker_registry

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Every agent worker kind gets its memory from one place, so no kind can be sized differently by accident.
func TestEveryWorkerKindJobCarriesTheSharedMemoryResources(t *testing.T) {
	for name, request := range map[string]proto.Message{
		"reflection":   &contracts.ReflectionRequest{},
		"incident":     &contracts.IncidentRequest{},
		"health-judge": &contracts.HealthJudgeRequest{},
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := anypb.New(request)
			if err != nil {
				t.Fatal(err)
			}
			task := reflectionTestTask(t)
			task.Request = payload
			client := fake.NewSimpleClientset()
			executor := KubernetesExecutor{Client: client, Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace"}
			launch := &contracts.KubernetesJobLaunch{Image: "example/worker:1", Command: []string{"/worker"}}
			jobName := JobName(task.TaskId)
			if err := executor.ensureJob(context.Background(), task, launch, jobName, requestSource{ConfigMap: jobName + "-request"}, jobName+"-result", ""); err != nil {
				t.Fatal(err)
			}
			job, err := client.BatchV1().Jobs("demo").Get(context.Background(), jobName, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			resources := job.Spec.Template.Spec.Containers[0].Resources
			if limit := resources.Limits[corev1.ResourceMemory]; limit.Cmp(resource.MustParse(WorkerMemoryLimit)) != 0 {
				t.Fatalf("memory limit = %s, want %s", limit.String(), WorkerMemoryLimit)
			}
			if request := resources.Requests[corev1.ResourceMemory]; request.Cmp(resource.MustParse(WorkerMemoryRequest)) != 0 {
				t.Fatalf("memory request = %s, want %s", request.String(), WorkerMemoryRequest)
			}
		})
	}
}

func oomFixture(t *testing.T, admittedLimit string) error {
	t.Helper()
	task := reflectionTestTask(t)
	name := JobName(task.TaskId)
	requested := corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(WorkerMemoryLimit)}}
	admitted := corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(admittedLimit)}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "worker", Namespace: "demo"},
		Spec:   batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Resources: requested}}}}},
		Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-pod", Namespace: "demo", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: name, UID: job.UID}}},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Resources: admitted}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "agent", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}}}}}
	executor := KubernetesExecutor{Client: fake.NewSimpleClientset(job, pod), Namespace: "demo", Registry: reflectionTestRegistry(t), RepositoryPVC: "repo", RepositoryMountPath: "/workspace"}
	_, err := executor.Execute(context.Background(), task)
	return err
}

// An OOM ends with a typed reason, not a bare backoff.
func TestOOMKilledWorkerEndsWithATypedReason(t *testing.T) {
	err := oomFixture(t, WorkerMemoryLimit)
	var failed *JobFailedError
	if !errors.As(err, &failed) || failed.FailureReason() != FailureWorkerOOMKilled {
		t.Fatalf("OOM not typed: %v", err)
	}
	if !strings.Contains(err.Error(), FailureWorkerOOMKilled) {
		t.Fatalf("typed reason missing from the failure text: %v", err)
	}
}

// A pod that was admitted with another memory limit than the Job asked for was rewritten by the cluster, so
// resizing the worker would not help; the failure names both limits.
func TestWorkerAdmittedWithAnotherMemoryLimitIsTypedAsRewritten(t *testing.T) {
	err := oomFixture(t, "16Mi")
	var failed *JobFailedError
	if !errors.As(err, &failed) || failed.FailureReason() != FailureWorkerResourcesRewritten {
		t.Fatalf("rewritten limit not typed: %v", err)
	}
	for _, want := range []string{"16Mi", WorkerMemoryLimit} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("failure text does not name %s: %v", want, err)
		}
	}
}

func TestOrdinaryWorkerFailureHasNoResourceReason(t *testing.T) {
	if reason := (&JobFailedError{Containers: []ContainerFailure{{ContainerName: "agent", Reason: "Error", ExitCode: 1}}}).FailureReason(); reason != "" {
		t.Fatalf("exit 1 typed as %q", reason)
	}
}
