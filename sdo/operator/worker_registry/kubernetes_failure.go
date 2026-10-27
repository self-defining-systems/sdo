package worker_registry

import (
	"context"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

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
			failed.Containers = append(failed.Containers, ContainerFailure{PodName: pod.Name, ContainerName: status.Name, Reason: failureText(termination.Reason), ExitCode: termination.ExitCode, Message: failureText(termination.Message)})
			if len(failed.Containers) == 16 {
				return failed
			}
		}
	}
	return failed
}
