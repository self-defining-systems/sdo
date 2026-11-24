package runtime

import (
	"maps"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

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
	// startupMaxUnseen bounds how long before its Ready transition a pod was
	// last seen not yet Ready; a longer gap could hide a first Ready and a flap.
	startupMaxUnseen = time.Minute
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
	// starting is when each pod whose containers have never been Ready was
	// last seen. A pod leaves it on its first Ready, a restart or its deletion.
	starting map[types.UID]time.Time
}

type workloadStartups struct {
	ready     []time.Duration
	lastReady time.Time
	// bound is the most any pod took when all were Ready before a start was
	// seen; it stands in for the missing starts and is never raised.
	bound time.Duration
}

// NewStartupObservations returns an empty store.
func NewStartupObservations() *StartupObservations {
	return &StartupObservations{workloads: map[string]*workloadStartups{}}
}

// Observe records the healthy starts among pods as seen at `at`: a pod seen
// before its containers were ever Ready that is now Ready with no container
// restart. Kubernetes keeps only the last Ready transition, so a pod first seen
// Ready, or that was Ready before, may have flapped and is no start; the pods a
// new workload is first seen with only bound its starts until one is seen.
// workload names a pod's workload (sdk.PodWorkload).
func (s *StartupObservations) Observe(at time.Time, pods []corev1.Pod, workload func(corev1.Pod) string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	starting, bounds := map[types.UID]time.Time{}, map[string]time.Duration{}
	for _, pod := range pods {
		started, ready, clean := podStart(pod)
		readyAt := ready.LastTransitionTime.Time
		switch seen, tracked := s.starting[pod.UID]; {
		case !clean:
		case ready.Status != corev1.ConditionTrue:
			// Never Ready: nothing started, or unready since before the start.
			// A pod that lost Ready carries the time it lost it instead.
			if started.IsZero() || !readyAt.IsZero() && !readyAt.After(started) {
				starting[pod.UID] = at
			}
		case started.IsZero() || readyAt.Before(started):
		case !tracked:
			bounds[workload(pod)] = max(bounds[workload(pod)], readyAt.Sub(started))
		case readyAt.Sub(seen) <= startupMaxUnseen:
			if w := s.workload(workload(pod)); w != nil {
				w.ready = append(w.ready, readyAt.Sub(started))
				w.ready = w.ready[max(0, len(w.ready)-startupSamplesPerWorkload):]
				w.lastReady = readyAt
			}
		}
	}
	s.starting = starting
	for name, bound := range bounds {
		if s.workloads[name] == nil {
			if w := s.workload(name); w != nil {
				w.bound = bound
			}
		}
	}
}

// workload is the named workload's record, created within startupMaxWorkloads.
func (s *StartupObservations) workload(name string) *workloadStartups {
	if s.workloads[name] == nil && len(s.workloads) < startupMaxWorkloads {
		s.workloads[name] = &workloadStartups{}
	}
	return s.workloads[name]
}

// podStart is when the pod's first container started (zero when none has) and
// its Ready condition. A pod being deleted or with a container that ended or
// restarted is not a clean start.
func podStart(pod corev1.Pod) (started time.Time, ready corev1.PodCondition, clean bool) {
	for _, status := range pod.Status.ContainerStatuses {
		if status.RestartCount > 0 || status.State.Terminated != nil || status.LastTerminationState.Terminated != nil {
			return time.Time{}, ready, false
		}
		if running := status.State.Running; running != nil && (started.IsZero() || running.StartedAt.Time.Before(started)) {
			started = running.StartedAt.Time
		}
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			ready = condition
		}
	}
	return started, ready, pod.DeletionTimestamp == nil
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
		sorted := w.samples()
		if len(sorted) == 0 {
			continue
		}
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

// samples are the observed starts, or the bound while there are none.
func (w *workloadStartups) samples() []time.Duration {
	if len(w.ready) == 0 && w.bound > 0 {
		return []time.Duration{w.bound}
	}
	return slices.Clone(w.ready)
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
		for _, d := range w.samples() {
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
		w := s.workload(item.GetWorkload())
		if w == nil {
			continue
		}
		var ready []time.Duration
		for _, d := range item.GetReadyAfter() {
			ready = append(ready, d.AsDuration())
		}
		if item.GetLastReadyAt() == nil {
			w.bound = slices.Max(append(ready, 0)) // no start was seen yet
			continue
		}
		w.ready = append(ready, w.ready...)
		w.ready = w.ready[max(0, len(w.ready)-startupSamplesPerWorkload):]
		if at := controllerTime(item.GetLastReadyAt()); at.After(w.lastReady) {
			w.lastReady = at
		}
	}
}
