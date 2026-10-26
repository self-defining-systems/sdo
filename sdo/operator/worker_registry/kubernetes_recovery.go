package worker_registry

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
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
	if e.Client == nil || e.Registry == nil || e.Namespace == "" {
		return nil, false, fmt.Errorf("agent recovery requires client, registry and namespace")
	}
	if err := e.Registry.ValidateTask(task); err != nil {
		return nil, false, fmt.Errorf("validate recovered task: %w", err)
	}
	name, err := e.taskJobName(ctx, task)
	if err != nil {
		return nil, false, err
	}
	resultMap, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, name+"-result", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, e.recoverMissingResult(ctx, task, name)
	}
	if err != nil {
		return nil, false, fmt.Errorf("read durable agent result: %w", err)
	}
	if digest := resultMap.Annotations[requestDigestAnnotation]; digest != "" && digest != taskRequestDigest(task) {
		if name == JobName(task.GetTaskId()) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("result ConfigMap %q request digest differs from frozen task", name)
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
			if name == JobName(task.GetTaskId()) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("durable agent request differs from frozen task")
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, fmt.Errorf("read frozen agent request: %w", err)
	} else {
		if resultMap.Annotations[requestDigestAnnotation] != taskRequestDigest(task) {
			return nil, false, fmt.Errorf("durable agent result has no matching frozen request")
		}
	}
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, name, metav1.GetOptions{})
	completedAt := time.Now()
	if err == nil {
		if jobFailed(job) {
			failure := e.taskJobFailure(ctx, task, job)
			if result.GetStatus() != "completed" || errors.Is(failure, context.DeadlineExceeded) {
				return nil, false, failure
			}
			// A later incarnation may have failed publishing over this exact
			// complete result. Terminal failure plus quiescence permits reuse;
			// an active or terminating worker still blocks recovery.
			if err := e.verifyWorkerQuiescence(ctx, name); err != nil {
				return nil, false, err
			}
			if !resultMap.CreationTimestamp.IsZero() {
				completedAt = resultMap.CreationTimestamp.Time
			}
		} else {
			if !jobCompleted(job) {
				return nil, false, nil
			}
			if job.Status.CompletionTime != nil && !job.Status.CompletionTime.IsZero() {
				completedAt = job.Status.CompletionTime.Time
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return nil, false, fmt.Errorf("inspect completed agent Job: %w", err)
	} else {
		if err := e.verifyWorkerQuiescence(ctx, name); err != nil {
			return nil, false, err
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

// taskJobName preserves legacy identities when their immutable request matches.
// A conflicting or unbound legacy result is retained as evidence, while a new
// request uses a content-addressed identity. Repeated restarts select that same
// identity, independently of timestamps or controller process lifetime.
func (e KubernetesExecutor) taskJobName(ctx context.Context, task *contracts.AgentTask) (string, error) {
	if binding, err := e.loadWarmBinding(task); err != nil {
		return "", err
	} else if binding != nil {
		return binding.JobName, nil
	}
	if identity, err := e.loadIncidentExecution(ctx, task); err != nil {
		return "", err
	} else if identity != nil {
		if strings.HasPrefix(identity.JobName, "sdo-standby-") {
			return "", errors.New("warm execution identity is missing its durable session binding")
		}
		return identity.JobName, nil
	}
	boundName := TaskJobName(task)
	maps := e.Client.CoreV1().ConfigMaps(e.Namespace)
	// Prefer the content-addressed execution even if legacy evidence is later
	// archived independently. Conflicting requests use this identity.
	for _, suffix := range []string{"-request", "-result", "-execution"} {
		if _, err := maps.Get(ctx, boundName+suffix, metav1.GetOptions{}); err == nil {
			return boundName, nil
		} else if !apierrors.IsNotFound(err) {
			return "", fmt.Errorf("inspect request-bound agent evidence: %w", err)
		}
	}
	name := JobName(task.GetTaskId())
	request, err := maps.Get(ctx, name+"-request", metav1.GetOptions{})
	if err == nil {
		prior := new(contracts.AgentTask)
		if wire.Unmarshal([]byte(request.Data[TaskDataKey]), prior) == nil && proto.Equal(prior, task) {
			result, err := maps.Get(ctx, name+"-result", metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return name, nil
			}
			if err != nil {
				return "", fmt.Errorf("inspect legacy agent result binding: %w", err)
			}
			if digest := result.Annotations[requestDigestAnnotation]; digest != "" && digest != taskRequestDigest(task) {
				return boundName, nil
			}
			return name, nil
		}
	} else if !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("inspect durable agent request: %w", err)
	} else {
		result, err := maps.Get(ctx, name+"-result", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return name, nil
		}
		if err != nil {
			return "", fmt.Errorf("inspect durable agent result: %w", err)
		}
		if result.Annotations[requestDigestAnnotation] == taskRequestDigest(task) {
			return name, nil
		}
	}
	return boundName, nil
}

const requestDigestAnnotation = "sdo.dev/request-sha256"

func taskRequestDigest(task *contracts.AgentTask) string {
	payload, _ := wire.Canonical(task) // validated task; canonical protobuf encoding cannot fail
	return fmt.Sprintf("%x", sha256.Sum256(payload))
}

// TaskJobName identifies an immutable validated task by its full request,
// including workspace, base revision and deadline, rather than just task ID.
func TaskJobName(task *contracts.AgentTask) string {
	return "sdo-task-" + taskRequestDigest(task)[:52]
}

func (e KubernetesExecutor) verifyWorkerQuiescence(ctx context.Context, name string) error {
	pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("verify agent worker quiescence: %w", err)
	}
	for _, pod := range pods.Items {
		owned := pod.Labels["sdo.dev/incident"] == name || pod.Labels["job-name"] == name || pod.Labels["batch.kubernetes.io/job-name"] == name
		for _, owner := range pod.OwnerReferences {
			owned = owned || owner.Kind == "Job" && owner.Name == name
		}
		if owned && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return fmt.Errorf("agent result awaits worker quiescence: Pod %s is %s", pod.Name, pod.Status.Phase)
		}
	}
	return nil
}
