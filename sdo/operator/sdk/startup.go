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

// ObservedStartupSource is implemented by detection contexts that carry how
// long each workload's pods took to become Ready on their recent healthy
// starts, as an allowance (a high percentile plus a margin, from the
// controller). Snapshot capture records it, so replay judges a starting pod
// exactly as the controller did.
type ObservedStartupSource interface {
	// ObservedStartup is the workload's ("Kind/name", see PodWorkload)
	// observed startup allowance; zero when none was observed.
	ObservedStartup(workload string) time.Duration
	// ObservedStartups lists every workload's allowance, for snapshot capture
	// and the memory-host wire.
	ObservedStartups() map[string]time.Duration
}

// PodWorkload names the workload a pod belongs to as "Kind/name": its
// controlling owner, or, for a ReplicaSet, the Deployment that owns that.
// A pod without a controlling owner is its own workload ("Pod/name").
func PodWorkload(ctx DetectionContext, pod corev1.Pod) string {
	for _, owner := range pod.OwnerReferences {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.Kind == "ReplicaSet" {
			for _, rs := range ctx.ReplicaSets() {
				if rs.Namespace != pod.Namespace || rs.Name != owner.Name {
					continue
				}
				for _, rsOwner := range rs.OwnerReferences {
					if rsOwner.Controller != nil && *rsOwner.Controller && rsOwner.Kind == "Deployment" {
						return "Deployment/" + rsOwner.Name
					}
				}
			}
		}
		return owner.Kind + "/" + owner.Name
	}
	return "Pod/" + pod.Name
}

// StartupAllowance is how long a pod's containers may take to become ready
// before the pod has outlived Kubernetes' own self-healing: the larger of what
// the container declares (ContainerStartupAllowance) and what the pod's
// workload was observed to need on its recent healthy starts. Zero means
// neither is known.
func StartupAllowance(ctx DetectionContext, pod corev1.Pod, name string) time.Duration {
	allowance := ContainerStartupAllowance(pod, name)
	if source, ok := ctx.(ObservedStartupSource); ok {
		if observed := source.ObservedStartup(PodWorkload(ctx, pod)); observed > allowance {
			allowance = observed
		}
	}
	return allowance
}

// StillStarting reports whether a running container is inside its startup
// allowance (StartupAllowance) at the observation time: it started less than
// its allowance ago. A container with no declared or observed allowance, an
// unknown start or an unknown observation time is never still starting.
func StillStarting(ctx DetectionContext, pod corev1.Pod, status corev1.ContainerStatus) bool {
	now := ObservationTime(ctx)
	if status.State.Running == nil || status.State.Running.StartedAt.IsZero() || now.IsZero() {
		return false
	}
	return now.Sub(status.State.Running.StartedAt.Time) < StartupAllowance(ctx, pod, status.Name)
}

// ResourceStartupAllowance is the longest startup allowance observed for the
// workloads the given resources belong to: a Pod resolves to its workload, any
// other resource names its own workload ("Kind/name"). A finding about a
// workload cannot clear before a replacement pod has had that long to become
// Ready. Zero means none is known.
func ResourceStartupAllowance(ctx DetectionContext, resources ...ObjectRef) time.Duration {
	source, ok := ctx.(ObservedStartupSource)
	if !ok {
		return 0
	}
	var allowance time.Duration
	for _, ref := range resources {
		workload := ref.Kind + "/" + ref.Name
		if ref.Kind == "Pod" {
			for _, pod := range ctx.Pods() {
				if pod.Namespace == ref.Namespace && pod.Name == ref.Name {
					workload = PodWorkload(ctx, pod)
				}
			}
		}
		allowance = max(allowance, source.ObservedStartup(workload))
	}
	return allowance
}

// PodStarting reports whether a pod that is not yet ready is still inside its
// startup: a running unready container within its startup allowance, or a
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
