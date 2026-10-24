package runtime

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
)

// IncidentWorkActive checks durable work before an expired request is retired.
// API uncertainty is an error, never evidence that an existing worker is gone.
func (d KubernetesJobDispatcher) IncidentWorkActive(ctx context.Context, incidentID string) (bool, error) {
	if d.Client == nil {
		return false, fmt.Errorf("incident worker client is required")
	}
	name := IncidentJobName(incidentID)
	job, err := d.Client.BatchV1().Jobs(d.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("inspect incident worker: %w", err)
	}
	if err == nil {
		// Deleting Jobs can still have running Pods. Await their removal.
		if job.Status.Active > 0 || job.DeletionTimestamp != nil {
			return true, nil
		}
		terminal := job.Status.CompletionTime != nil
		for _, condition := range job.Status.Conditions {
			terminal = terminal || condition.Status == corev1.ConditionTrue && (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed)
		}
		if !terminal {
			return true, nil
		}
	}
	// A Job can disappear before its orphaned Pod does. Do not retire the
	// incident while any worker container or tool sidecar can still act.
	pods, err := d.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, fmt.Errorf("inspect incident worker Pods: %w", err)
	}
	for _, pod := range pods.Items {
		owned := pod.Labels["sdo.dev/incident"] == name || pod.Labels["job-name"] == name || pod.Labels["batch.kubernetes.io/job-name"] == name
		for _, owner := range pod.OwnerReferences {
			owned = owned || owner.Kind == "Job" && owner.Name == name
		}
		if owned && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return true, nil
		}
	}
	return false, nil
}

// retireExpiredIncident refuses to resume an abandoned request after its
// response deadline. Terminal evidence goes through the existing typed failure
// archive, so a deleted proposal cannot block the next incident's dispatch.
func (c *Controller) retireExpiredIncident(ctx context.Context, now time.Time) error {
	c.mu.Lock()
	request := cloneIncidentRequest(c.currentIncidentRequest)
	eligible := c.canRetireExpiredIncidentLocked(now)
	c.mu.Unlock()
	if !eligible {
		return nil
	}
	if checker, ok := c.dispatcher.(interface {
		IncidentWorkActive(context.Context, string) (bool, error)
	}); ok {
		active, err := checker.IncidentWorkActive(ctx, request.IncidentID)
		if err != nil {
			return err
		}
		if active {
			return nil
		}
	}
	c.mu.Lock()
	if c.currentIncidentRequest == nil || c.currentIncidentRequest.IncidentID != request.IncidentID || !c.canRetireExpiredIncidentLocked(now) {
		c.mu.Unlock()
		return nil
	}
	closure := IncidentClosure{
		Request: *request, Result: cloneIncidentResult(c.currentIncidentResult),
		ObservationGeneration: request.ObservationGeneration,
		DispatchError:         "response deadline expired with no live incident work",
		DetectedAt:            c.incidentDetectedAt, DispatchedAt: c.incidentDispatchedAt,
		ResponderCompletedAt: now, VerifiedAt: now, DetectorTimeline: cloneTimeline(c.timeline),
	}
	projected, err := incidentClosureContract(closure)
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Begin, At: now, Closure: projected})
	}
	if err == nil && (c.pendingClosure == nil || c.pendingClosure.Request.IncidentID != request.IncidentID || c.closureState != contracts.ClosureState_CLOSURE_STATE_PENDING) {
		err = fmt.Errorf("cannot retire expired incident while another closure is pending")
	}
	if err == nil {
		_, err = c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.ResponseExpired, At: now, Error: closure.DispatchError})
	}
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.emitLifecycleLocked(&contracts.LifecycleEvent{Event: PhaseClosed, RecordedAt: contractTimestamp(now), IncidentId: request.IncidentID, DispatchError: true})
	failure := cloneClosureFailure(c.closureFailure)
	c.clearIncidentLocked()
	c.timeline = nil
	// Fresh observations of a still-firing detector must activate a new incident.
	c.tracker.Restore(nil)
	c.batcher.Restore(BatcherState{})
	c.scheduler.ExpediteAll(now)
	c.mu.Unlock()
	if err := c.PersistState(ctx); err != nil {
		return err
	}
	if failure != nil && c.OnClosureFailed != nil {
		c.OnClosureFailed(*failure)
	}
	if c.OnIncidentClosed != nil {
		c.OnIncidentClosed(closure)
	}
	return nil
}

// canRetireExpiredIncidentLocked leaves claimed closures with their owner,
// including terminal failures whose original reason and notification must survive.
// A failed closure from an earlier incident does not claim the current incident.
func (c *Controller) canRetireExpiredIncidentLocked(now time.Time) bool {
	request := c.currentIncidentRequest
	if !c.incidentOpen || request == nil || request.ResponseDeadline.IsZero() || now.Before(request.ResponseDeadline) || c.responderDone ||
		c.dispatchState == contracts.DispatchState_DISPATCH_STATE_RUNNING || c.dispatchState == contracts.DispatchState_DISPATCH_STATE_WORKSPACE_RUNNING {
		return false
	}
	if c.closureBlocksNextLocked() || c.pendingClosure != nil && c.pendingClosure.Request.IncidentID == request.IncidentID ||
		c.closureFailure != nil && c.closureFailure.IncidentID == request.IncidentID {
		return false
	}
	return true
}

func (c *Controller) clearIncidentLocked() {
	c.incidentOpen = false
	c.responderDone = false
	c.currentIncidentRequest = nil
	c.currentIncidentResult = nil
	c.dispatchError = ""
	c.incidentDetectedAt = time.Time{}
	c.incidentDispatchedAt = time.Time{}
	c.responderCompletedAt = time.Time{}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_IDLE
	c.incidentFindingKeys = nil
	c.cleanedHelpers = nil
	c.resetDispatchRetryLocked()
}
