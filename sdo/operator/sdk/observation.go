package sdk

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObservationClock is implemented by detection contexts that record when
// their objects were read. The controller stamps each snapshot once, when it
// builds it, and corpus captures carry the stamp so replay sees the same time.
type ObservationClock interface {
	ObservedAt() time.Time
}

// ObservationTime returns the time a detection context describes. Detectors
// that judge an age ("Pending longer than a minute", "restarted within five
// minutes") must use it instead of time.Now, so Detect stays a pure function
// of its snapshot and replays identically.
//
// A context without a recorded clock falls back to the latest timestamp
// recorded in its Pods, Deployments, ReplicaSets, and Events. That is a
// deterministic lower bound on the real observation time, so an age derived
// from it can only be underestimated: a grace period may elapse late, never
// early. It is zero when the context records no time at all.
func ObservationTime(ctx DetectionContext) time.Time {
	if clock, ok := ctx.(ObservationClock); ok {
		if observed := clock.ObservedAt(); !observed.IsZero() {
			return observed
		}
	}
	var latest time.Time
	consider := func(value time.Time) {
		if value.After(latest) {
			latest = value
		}
	}
	considerMeta := func(value metav1.Time) { consider(value.Time) }
	considerState := func(state corev1.ContainerState) {
		if state.Running != nil {
			considerMeta(state.Running.StartedAt)
		}
		if state.Terminated != nil {
			considerMeta(state.Terminated.StartedAt)
			considerMeta(state.Terminated.FinishedAt)
		}
	}
	for _, pod := range ctx.Pods() {
		considerMeta(pod.CreationTimestamp)
		if pod.Status.StartTime != nil {
			considerMeta(*pod.Status.StartTime)
		}
		for _, condition := range pod.Status.Conditions {
			considerMeta(condition.LastProbeTime)
			considerMeta(condition.LastTransitionTime)
		}
		for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
			for _, status := range statuses {
				considerState(status.State)
				considerState(status.LastTerminationState)
			}
		}
	}
	for _, deployment := range ctx.Deployments() {
		considerMeta(deployment.CreationTimestamp)
		for _, condition := range deployment.Status.Conditions {
			considerMeta(condition.LastUpdateTime)
			considerMeta(condition.LastTransitionTime)
		}
	}
	for _, replicaSet := range ctx.ReplicaSets() {
		considerMeta(replicaSet.CreationTimestamp)
	}
	for _, event := range ctx.Events() {
		considerMeta(event.CreationTimestamp)
		considerMeta(event.FirstTimestamp)
		considerMeta(event.LastTimestamp)
		consider(event.EventTime.Time)
		if event.Series != nil {
			consider(event.Series.LastObservedTime.Time)
		}
	}
	return latest
}
