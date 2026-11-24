// Package workloadreadiness dates aggregate replica deficits from Kubernetes observations.
package workloadreadiness

import (
	"time"

	core "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// Deficit reports whether the replica deficit outlived self-healing. The
// controller transition, if supplied, can date the deficit, but newer owned
// Pod activity starts a new recovery opportunity. A workload's creation does
// not date a shortage when all evidence of the affected Pods is gone.
func Deficit(ctx sdk.DetectionContext, since time.Time, pods []core.Pod) (time.Time, bool) {
	starting := false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			since = later(since, pod.DeletionTimestamp.Time)
			continue
		}
		var readySince time.Time
		for _, condition := range pod.Status.Conditions {
			if condition.Type == core.PodReady {
				if condition.Status == core.ConditionTrue {
					readySince = later(pod.CreationTimestamp.Time, condition.LastTransitionTime.Time)
				} else {
					since = later(since, condition.LastTransitionTime.Time)
				}
				break
			}
		}
		if !readySince.IsZero() {
			// A ready sibling cannot date a missing replica. It can still
			// explain lagging controller status after recent recovery.
			starting = starting || !sdk.HeldPastSelfHeal(ctx, readySince)
			continue
		}
		since = later(since, pod.CreationTimestamp.Time)
		starting = starting || sdk.PodStarting(ctx, pod)
	}
	return since, !starting && sdk.HeldPastSelfHeal(ctx, since)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
