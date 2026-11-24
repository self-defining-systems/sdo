// Package workloadreadiness dates aggregate replica deficits from Kubernetes observations.
package workloadreadiness

import (
	"sync"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// Tracker keeps deficit timestamps for one detector's observed workloads.
type Tracker struct {
	mu       sync.Mutex
	at       time.Time
	deficits map[workload]time.Time
}

type workload struct {
	kind, namespace, name string
	uid                   types.UID
	generation            int64
}

// Observation replaces the previous set of deficits, so recovery and workload
// removal reset the clock. Older snapshots cannot overwrite newer state.
type Observation struct {
	previous, current map[workload]time.Time
}

func (t *Tracker) Observe(ctx sdk.DetectionContext, evaluate func(*Observation)) {
	at := sdk.ObservationTime(ctx)
	t.mu.Lock()
	defer t.mu.Unlock()
	observation := Observation{current: make(map[workload]time.Time)}
	if !at.Before(t.at) {
		observation.previous = t.deficits
	}
	evaluate(&observation)
	if !at.IsZero() && !at.Before(t.at) {
		t.at, t.deficits = at, observation.current
	}
}

// Deficit reports whether this deficit outlived self-healing. Use a condition
// transition or recent Pod activity when available; otherwise date the deficit
// from its first observation. Never fall back to the workload's creation.
func (o *Observation) Deficit(ctx sdk.DetectionContext, kind string, object meta.Object, since time.Time, pods []core.Pod) (time.Time, bool) {
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
	key := workload{kind: kind, namespace: object.GetNamespace(), name: object.GetName(), uid: object.GetUID(), generation: object.GetGeneration()}
	if since.IsZero() {
		since = o.previous[key]
		if since.IsZero() {
			since = sdk.ObservationTime(ctx)
		}
	}
	o.current[key] = since
	return since, !starting && sdk.HeldPastSelfHeal(ctx, since)
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
