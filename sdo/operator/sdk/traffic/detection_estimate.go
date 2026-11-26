package traffic

import (
	"math"
	"time"
)

// observeDelay is what follows a confirmed failure before it opens an
// incident: the controller reads the prober every 500 ms, the health detector
// fires on its second evaluation (Firing 2, one per read while a sample
// fails), and the opener batches findings for 500 ms.
const observeDelay = 1500 * time.Millisecond

// DetectionEstimate is how long a health probe takes to notice a scenario
// whose every request starts failing, through the fast confirmation path
// (ConfirmedFailures). It is computed from the workload's own schedule and
// SDO's confirmation rules, never measured.
type DetectionEstimate struct {
	// Every is the mean time between the scenario's scheduled samples.
	Every time.Duration
	// Typical is for a failure at a random moment whose requests fail about
	// as fast as a healthy run.
	Typical time.Duration
	// Worst is for a failure at the worst moment of the schedule whose
	// requests hang until the request timeout.
	Worst time.Duration
}

// ExpectedDetection estimates how long the workload takes to notice the
// scenario failing, given the slowest healthy run of it: the wait for the
// scenario's next scheduled sample (the gaps between its slots in the
// SamplingPlan; a Poisson gap averages its mean and is capped at four), that
// sample failing, the confirming samples, and observeDelay. It is zero for a
// scenario the workload does not schedule, a known-broken one, and any
// workload that is not a health probe: none of them notices. A partial
// failure is left to the SLO and takes longer.
func (w Workload) ExpectedDetection(id string, healthy time.Duration) DetectionEstimate {
	if w.Purpose != PurposeHealthProbe || w.KnownBrokenScenario(id) != nil {
		return DetectionEstimate{}
	}
	plan := w.SamplingPlan()
	var slots []int
	for i, scheduled := range plan.Schedule {
		if scheduled == id {
			slots = append(slots, i)
		}
	}
	if plan.Rate <= 0 || len(slots) == 0 {
		return DetectionEstimate{}
	}
	slot := float64(time.Second) / plan.Rate
	every := time.Duration(slot * float64(len(plan.Schedule)) / float64(len(slots)))
	var meanWait, worstWait time.Duration
	if w.Arrival == ArrivalPoisson {
		meanWait, worstWait = every, 4*every
	} else {
		// A failure at a uniformly random moment waits half the gap it falls
		// in; longer gaps catch more moments.
		cycle := float64(len(plan.Schedule))
		var squares, longest float64
		for i, at := range slots {
			next := float64(slots[(i+1)%len(slots)])
			if next <= float64(at) {
				next += cycle
			}
			gap := next - float64(at)
			squares += gap * gap
			longest = math.Max(longest, gap)
		}
		meanWait = time.Duration(slot * squares / (2 * cycle))
		worstWait = time.Duration(slot * longest)
	}
	hang := min(healthy+w.Timeout.Duration(), w.IterationTimeout.Duration())
	return DetectionEstimate{
		Every:   every,
		Typical: meanWait + confirmationDelay(healthy) + observeDelay,
		Worst:   worstWait + confirmationDelay(hang) + observeDelay,
	}
}

// confirmationDelay is how long after a failing scheduled sample starts the
// failure is confirmed, when every sample fails after failing: the confirmer
// samples again confirmDelay after the first ends, and once more
// confirmStallSpan after it began unless the failure spans that already; the
// failure is confirmed when the sample that spans confirmStallSpan ends.
func confirmationDelay(failing time.Duration) time.Duration {
	again := failing + confirmDelay
	if again >= confirmStallSpan {
		return again + failing
	}
	return max(confirmStallSpan, again+failing) + failing
}
