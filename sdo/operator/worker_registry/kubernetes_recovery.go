package worker_registry

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// ReflectionResultRecoverer reads terminal evidence without executing a worker.
// It remains usable after a frozen execution deadline has expired.
type ReflectionResultRecoverer interface {
	RecoverCompleted(context.Context, *contracts.AgentTask) (*contracts.AgentResult, bool, error)
}

// RecoverCompleted checks the durable result before any Job creation. A worker
// publishes before exiting, so a live Job must independently be complete. Once
// TTL removes the Job, absence of live owned Pods establishes quiescence. The
// server's result creation time preserves the original execution deadline.
func (e KubernetesExecutor) RecoverCompleted(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, bool, error) {
	if !task.GetRequest().MessageIs(&contracts.ReflectionRequest{}) {
		return nil, false, nil
	}
	if e.Client == nil || e.Registry == nil || e.Namespace == "" {
		return nil, false, fmt.Errorf("reflection recovery requires client, registry and namespace")
	}
	if err := e.Registry.ValidateTask(task); err != nil {
		return nil, false, fmt.Errorf("validate recovered task: %w", err)
	}
	name := JobName(task.GetTaskId())
	resultMap, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, name+"-result", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read durable reflection result: %w", err)
	}
	data, ok := resultMap.Data[ResultDataKey]
	if !ok {
		return nil, false, fmt.Errorf("result ConfigMap %q has no %s", resultMap.Name, ResultDataKey)
	}
	result := new(contracts.AgentResult)
	if err := wire.Unmarshal([]byte(data), result); err != nil {
		return nil, false, fmt.Errorf("decode recovered result: %w", err)
	}
	if err := e.Registry.ValidateResult(task, result); err != nil {
		return nil, false, fmt.Errorf("validate recovered result: %w", err)
	}
	// Bind recovery to the same provisioned task, including payload/workspace.
	request, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, name+"-request", metav1.GetOptions{})
	if err == nil {
		prior := new(contracts.AgentTask)
		if err := wire.Unmarshal([]byte(request.Data[TaskDataKey]), prior); err != nil || !proto.Equal(prior, task) {
			return nil, false, fmt.Errorf("durable reflection request differs from frozen task")
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, fmt.Errorf("read frozen reflection request: %w", err)
	} else {
		payload, err := wire.Canonical(task)
		if err != nil {
			return nil, false, err
		}
		if len(payload) <= MaxConfigMapRequestBytes {
			return nil, false, fmt.Errorf("durable reflection result has no frozen request")
		}
		// Large requests were staged on the narrow PVC, then removed at return.
		// Their caller retains the immutable typed task and result identity binding.
	}
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, name, metav1.GetOptions{})
	completedAt := time.Now()
	if err == nil {
		if jobFailed(job) {
			return nil, false, taskJobFailure(task, job)
		}
		if !jobCompleted(job) {
			return nil, false, nil
		}
		if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() {
			completedAt = job.Status.CompletionTime.Time
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, fmt.Errorf("inspect completed reflection Job: %w", err)
	} else {
		pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("verify reflection worker quiescence: %w", err)
		}
		for _, pod := range pods.Items {
			owned := pod.Labels["sdo.dev/incident"] == name || pod.Labels["job-name"] == name || pod.Labels["batch.kubernetes.io/job-name"] == name
			for _, owner := range pod.OwnerReferences {
				owned = owned || owner.Kind == "Job" && owner.Name == name
			}
			if owned && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				return nil, false, fmt.Errorf("reflection result awaits worker quiescence: Pod %s is %s", pod.Name, pod.Status.Phase)
			}
		}
		if !resultMap.CreationTimestamp.IsZero() {
			completedAt = resultMap.CreationTimestamp.Time
		}
	}
	if deadline, bounded := frozenReflectionDeadline(task); bounded && !completedAt.Before(deadline) {
		return nil, false, &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
	}
	return result, true, nil
}
