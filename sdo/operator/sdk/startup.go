package sdk

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Kubernetes defaults for a probe that declares a handler but not every field.
const (
	defaultProbePeriodSeconds      = 10
	defaultProbeFailureThreshold   = 3
	defaultStartupFailureThreshold = defaultProbeFailureThreshold
)

// ContainerStartupAllowance is how long a container may take to become ready
// by its own declaration: a startup probe's initialDelaySeconds plus
// failureThreshold x periodSeconds (the time the kubelet gives it before
// restarting it), or, with none, a readiness probe's initialDelaySeconds plus
// the same product. Zero means the container declares no allowance.
func ContainerStartupAllowance(pod corev1.Pod, name string) time.Duration {
	for _, c := range pod.Spec.Containers {
		if c.Name != name {
			continue
		}
		var allowance time.Duration
		for _, probe := range []*corev1.Probe{c.StartupProbe, c.ReadinessProbe} {
			if probe == nil {
				continue
			}
			period, threshold := int64(probe.PeriodSeconds), int64(probe.FailureThreshold)
			if period <= 0 {
				period = defaultProbePeriodSeconds
			}
			if threshold <= 0 {
				threshold = defaultStartupFailureThreshold
			}
			if d := time.Duration(int64(probe.InitialDelaySeconds)+period*threshold) * time.Second; d > allowance {
				allowance = d
			}
		}
		return allowance
	}
	return 0
}

// StillStarting reports whether a running container is inside its own
// declared startup allowance at the observation time: it started less than
// its allowance ago. A container with no declared allowance, an unknown start
// or an unknown observation time is never still starting.
func StillStarting(ctx DetectionContext, pod corev1.Pod, status corev1.ContainerStatus) bool {
	now := ObservationTime(ctx)
	if status.State.Running == nil || status.State.Running.StartedAt.IsZero() || now.IsZero() {
		return false
	}
	return now.Sub(status.State.Running.StartedAt.Time) < ContainerStartupAllowance(pod, status.Name)
}

// PodStarting reports whether a pod that is not yet ready is still inside its
// startup: a running unready container within its declared allowance, or a
// pod whose containers have not started yet within the self-heal hold of its
// creation (image pull, scheduling, init containers).
func PodStarting(ctx DetectionContext, pod corev1.Pod) bool {
	now := ObservationTime(ctx)
	if now.IsZero() || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return false
	}
	if len(pod.Status.ContainerStatuses) == 0 {
		return !pod.CreationTimestamp.IsZero() && now.Sub(pod.CreationTimestamp.Time) < SelfHealHold(ctx)
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Ready {
			continue
		}
		if status.State.Running != nil {
			if StillStarting(ctx, pod, status) {
				return true
			}
		} else if w := status.State.Waiting; w != nil && (w.Reason == "ContainerCreating" || w.Reason == "PodInitializing") &&
			!pod.CreationTimestamp.IsZero() && now.Sub(pod.CreationTimestamp.Time) < SelfHealHold(ctx) {
			return true
		}
	}
	return false
}
