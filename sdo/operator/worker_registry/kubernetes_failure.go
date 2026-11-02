package worker_registry

import (
	"context"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	// WorkerMemoryRequest and WorkerMemoryLimit size the container of every agent worker kind (responder,
	// reflection, health judge, deployer): they all go through ensureJob. An idle worker uses under 100 MiB
	// (87 MiB measured); the limit leaves headroom for the agent CLI, tool output and the session transcript.
	// A worker never hit this limit in recorded runs: the one OOM seen was a 16Mi limit rewritten at admission.
	WorkerMemoryRequest = "512Mi"
	WorkerMemoryLimit   = "4Gi"

	// FailureWorkerOOMKilled: the worker used more memory than its limit.
	FailureWorkerOOMKilled = "worker_oom_killed"
	// FailureWorkerResourcesRewritten: the worker was killed for memory, but its pod was admitted with another
	// limit than the Job asked for, so the cluster (an admission webhook) rewrote it and resizing does not help.
	FailureWorkerResourcesRewritten = "worker_resources_rewritten"
)

// FailureReason types the failure when a worker container was killed for memory, and is empty for any other failure.
func (err *JobFailedError) FailureReason() string {
	for _, container := range err.Containers {
		if container.Reason != "OOMKilled" {
			continue
		}
		if container.MemoryLimit != "" && err.RequestedMemoryLimit != "" && !sameQuantity(container.MemoryLimit, err.RequestedMemoryLimit) {
			return FailureWorkerResourcesRewritten
		}
		return FailureWorkerOOMKilled
	}
	return ""
}

func sameQuantity(a, b string) bool {
	x, errA := resource.ParseQuantity(a)
	y, errB := resource.ParseQuantity(b)
	return errA == nil && errB == nil && x.Cmp(y) == 0
}

// Bound status text before it enters controller logs or durable diagnostics.
// The controller reads no logs, environment, volumes or Secrets: a worker's termination
// message is the one curated line it wrote to /dev/termination-log. Invalid UTF-8 is repaired.
func failureText(value string) string {
	const maxRunes = 512
	text := []rune(strings.ToValidUTF8(value, "\uFFFD"))
	if len(text) > maxRunes {
		return string(text[:maxRunes]) + "…"
	}
	return string(text)
}

func (e KubernetesExecutor) taskJobFailure(ctx context.Context, task *contracts.AgentTask, job *batchv1.Job) error {
	failure := taskJobFailure(task, job)
	failed, ok := failure.(*JobFailedError)
	if !ok {
		return failure
	} // Preserve the distinct frozen-deadline branch.
	for _, condition := range job.Status.Conditions {
		if len(failed.Conditions) == 8 {
			break
		}
		failed.Conditions = append(failed.Conditions, JobFailureCondition{Type: string(condition.Type), Status: string(condition.Status), Reason: failureText(condition.Reason), Message: failureText(condition.Message)})
	}
	failed.RequestedMemoryLimit = containerMemoryLimit(job.Spec.Template.Spec.Containers, "agent")
	pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		failed.EvidenceError = failureText(err.Error())
		return failed
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	for _, pod := range pods.Items {
		owned := false
		for _, owner := range pod.OwnerReferences {
			if job.UID != "" && owner.UID == job.UID && owner.Kind == "Job" && owner.Name == job.Name {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		statuses := append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...)
		sort.Slice(statuses, func(i, j int) bool { return statuses[i].Name < statuses[j].Name })
		for _, status := range statuses {
			termination := status.State.Terminated
			if termination == nil {
				continue
			}
			failed.Containers = append(failed.Containers, ContainerFailure{PodName: pod.Name, ContainerName: status.Name, Reason: failureText(termination.Reason), ExitCode: termination.ExitCode, Message: failureText(termination.Message), MemoryLimit: containerMemoryLimit(pod.Spec.Containers, status.Name)})
			if len(failed.Containers) == 16 {
				return failed
			}
		}
	}
	return failed
}

func containerMemoryLimit(containers []corev1.Container, name string) string {
	for _, container := range containers {
		if container.Name == name {
			limit := container.Resources.Limits[corev1.ResourceMemory]
			if limit.IsZero() {
				return ""
			}
			return limit.String()
		}
	}
	return ""
}
