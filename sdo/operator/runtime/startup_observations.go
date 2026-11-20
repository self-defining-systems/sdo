package runtime

import (
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

const (
	// startupSamplesPerWorkload bounds the healthy starts kept per workload.
	startupSamplesPerWorkload = 20
	// startupMaxWorkloads bounds the workloads tracked.
	startupMaxWorkloads = 200
	// startupPercentile is the share of recent healthy starts the allowance
	// covers; startupMargin and startupSlack widen it.
	startupPercentile = 0.95
	startupMargin     = 1.25
	startupSlack      = 10 * time.Second
	// startupMaxAllowance caps what a workload may be allowed, so a
	// pathological history never hides a real outage for long.
	startupMaxAllowance = 15 * time.Minute
)

// StartupObservations remembers how long each workload's pods took from a
// container starting to the pod becoming Ready, over their recent healthy
// starts. A workload that declares no startup timing, or less than it needs,
// is judged starting for this long instead of for the generic self-heal hold.
// It is persisted in the controller state (the install's lifetime), so a
// restart keeps what was learned. Safe for concurrent use.
type StartupObservations struct {
	mu        sync.Mutex
	workloads map[string]*workloadStartups
}

type workloadStartups struct {
	ready     []time.Duration
	lastReady time.Time
}

// NewStartupObservations returns an empty store.
func NewStartupObservations() *StartupObservations {
	return &StartupObservations{workloads: map[string]*workloadStartups{}}
}

// Observe records the healthy starts among pods: a Running pod whose Ready
// condition turned true with no container restart. Each Ready transition is
// sampled once. workload names a pod's workload (sdk.PodWorkload).
func (s *StartupObservations) Observe(pods []corev1.Pod, workload func(corev1.Pod) string) {
	if s == nil {
		return
	}
	type sample struct {
		workload string
		readyAt  time.Time
		after    time.Duration
	}
	var samples []sample
	for _, pod := range pods {
		readyAt, after, ok := podStartToReady(pod)
		if ok {
			samples = append(samples, sample{workload(pod), readyAt, after})
		}
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].readyAt.Before(samples[j].readyAt) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range samples {
		w := s.workloads[item.workload]
		if w == nil {
			if len(s.workloads) >= startupMaxWorkloads {
				continue
			}
			w = &workloadStartups{}
			s.workloads[item.workload] = w
		}
		if !item.readyAt.After(w.lastReady) {
			continue
		}
		w.lastReady = item.readyAt
		w.ready = append(w.ready, item.after)
		if len(w.ready) > startupSamplesPerWorkload {
			w.ready = w.ready[len(w.ready)-startupSamplesPerWorkload:]
		}
	}
}

// podStartToReady is the time from a Running pod's first container start to
// its Ready transition. A pod that restarted a container, or whose Ready
// transition precedes a container start, is not a clean start.
func podStartToReady(pod corev1.Pod) (readyAt time.Time, after time.Duration, ok bool) {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) == 0 {
		return time.Time{}, 0, false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			readyAt = condition.LastTransitionTime.Time
		}
	}
	if readyAt.IsZero() {
		return time.Time{}, 0, false
	}
	var started time.Time
	for _, status := range pod.Status.ContainerStatuses {
		if status.RestartCount > 0 || status.State.Running == nil || status.State.Running.StartedAt.IsZero() {
			return time.Time{}, 0, false
		}
		if at := status.State.Running.StartedAt.Time; started.IsZero() || at.Before(started) {
			started = at
		}
	}
	if readyAt.Before(started) {
		return time.Time{}, 0, false
	}
	return readyAt, readyAt.Sub(started), true
}

// Allowances returns each observed workload's startup allowance: the
// startupPercentile of its recent healthy starts widened by a margin and
// capped.
func (s *StartupObservations) Allowances() map[string]time.Duration {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out map[string]time.Duration
	for name, w := range s.workloads {
		if len(w.ready) == 0 {
			continue
		}
		sorted := slices.Clone(w.ready)
		slices.Sort(sorted)
		rank := int(float64(len(sorted))*startupPercentile+0.999999) - 1
		rank = max(0, min(rank, len(sorted)-1))
		allowance := min(time.Duration(float64(sorted[rank])*startupMargin)+startupSlack, startupMaxAllowance)
		if out == nil {
			out = map[string]time.Duration{}
		}
		out[name] = allowance
	}
	return out
}

// workloadName names a pod's workload against a snapshot's ReplicaSets.
func workloadName(ctx sdk.DetectionContext) func(corev1.Pod) string {
	return func(pod corev1.Pod) string { return sdk.PodWorkload(ctx, pod) }
}

func (s *StartupObservations) contract() []*contracts.StartupObservation {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*contracts.StartupObservation
	for _, name := range slices.Sorted(maps.Keys(s.workloads)) {
		w := s.workloads[name]
		item := &contracts.StartupObservation{Workload: name, LastReadyAt: optionalTimestamp(w.lastReady)}
		for _, d := range w.ready {
			item.ReadyAfter = append(item.ReadyAfter, durationpb.New(d))
		}
		out = append(out, item)
	}
	return out
}

// restore merges persisted observations into the store: samples already
// observed in this process stay newest.
func (s *StartupObservations) restore(items []*contracts.StartupObservation) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range items {
		if len(s.workloads) >= startupMaxWorkloads && s.workloads[item.GetWorkload()] == nil {
			continue
		}
		var ready []time.Duration
		for _, d := range item.GetReadyAfter() {
			ready = append(ready, d.AsDuration())
		}
		w := s.workloads[item.GetWorkload()]
		if w == nil {
			w = &workloadStartups{}
			s.workloads[item.GetWorkload()] = w
		}
		w.ready = append(ready, w.ready...)
		if len(w.ready) > startupSamplesPerWorkload {
			w.ready = w.ready[len(w.ready)-startupSamplesPerWorkload:]
		}
		if at := controllerTime(item.GetLastReadyAt()); at.After(w.lastReady) {
			w.lastReady = at
		}
	}
}
