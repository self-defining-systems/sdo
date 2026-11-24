// Package workloadreadiness dates aggregate replica deficits from Kubernetes observations.
package workloadreadiness

import (
	"time"

	core "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// Deficit reports when a deficit began and whether it outlived self-healing.
// Use Pod startup or rollout progress when observed, otherwise use the
// caller's condition or creation timestamp. Ready siblings only explain a
// deficit when the Pods have caught up to the desired count and status lags.
func Deficit(ctx sdk.DetectionContext, fallback time.Time, desired int32, pods []core.Pod) (time.Time, bool) {
	var progress, readySince time.Time
	var ready int32
	starting := false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			progress = later(progress, pod.DeletionTimestamp.Time)
			continue
		}
		since := pod.CreationTimestamp.Time
		if pod.Status.StartTime != nil {
			since = later(since, pod.Status.StartTime.Time)
		}
		isReady := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == core.PodReady {
				since = later(since, condition.LastTransitionTime.Time)
				isReady = condition.Status == core.ConditionTrue
				break
			}
		}
		if isReady {
			ready++
			readySince = later(readySince, since)
			continue
		}
		progress = later(progress, since)
		starting = starting || sdk.PodStarting(ctx, pod)
	}
	if ready >= desired {
		progress = later(progress, readySince)
	}
	since := fallback
	if !progress.IsZero() {
		since = progress
	}
	return since, !starting && sdk.HeldPastSelfHeal(ctx, since)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
