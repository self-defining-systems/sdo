package worker_registry

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// JobVanishedError is terminal for a dispatched immutable execution. Recreating
// its Job could repeat effects after the original worker lost its result.
type JobVanishedError struct {
	TaskID  string
	JobName string
}

func (err *JobVanishedError) Error() string {
	return fmt.Sprintf("worker Job vanished: %q for task %q has no durable result", err.JobName, err.TaskID)
}
func (err *JobVanishedError) NonRetryable() bool { return true }

// Record successful dispatch separately from request preparation. This receipt
// survives Job TTL and prepared-task cleanup; a prepared request alone never
// proves that a worker ran. It has no Job owner reference intentionally.
func (e KubernetesExecutor) recordWorkerDispatch(ctx context.Context, task *contracts.AgentTask, name string) error {
	immutable := true
	receipt := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-execution", Annotations: map[string]string{requestDigestAnnotation: taskRequestDigest(task)}}, Immutable: &immutable}
	_, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Create(ctx, receipt, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		prior, readErr := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, receipt.Name, metav1.GetOptions{})
		if readErr != nil {
			return fmt.Errorf("read worker dispatch receipt: %w", readErr)
		}
		if prior.Annotations[requestDigestAnnotation] != taskRequestDigest(task) {
			return fmt.Errorf("worker dispatch receipt %q differs from frozen task", name)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("record worker dispatch receipt: %w", err)
	}
	return nil
}

// Missing results are ordinary before dispatch or while the Job exists. A
// confirmed missing Job after recorded dispatch is a permanent transport loss,
// including when recovery is the first operation after controller restart.
func (e KubernetesExecutor) recoverMissingResult(ctx context.Context, task *contracts.AgentTask, name string) error {
	if deadline, bounded := frozenReflectionDeadline(task); bounded && !time.Now().Before(deadline) {
		return &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
	}
	_, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("inspect agent Job without result: %w", err)
	}
	receipt, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, name+"-execution", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read worker dispatch receipt: %w", err)
	}
	if receipt.Annotations[requestDigestAnnotation] != taskRequestDigest(task) {
		return fmt.Errorf("worker dispatch receipt %q differs from frozen task", name)
	}
	return &JobVanishedError{TaskID: task.GetTaskId(), JobName: name}
}
