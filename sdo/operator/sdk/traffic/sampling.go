package traffic

import (
	"math"
	"time"
)

const (
	// MinSampleInterval is how often a health probe samples every one of its
	// scenarios at least, whatever its weight. The SLO needs MinSamples (3 by
	// default) failing samples inside MaxAge (30 s by default), and a page
	// waits for them: three samples 10 s apart span 20 s of a 30 s window and
	// confirm a failure within 30 s of it starting, where a weight-1 scenario
	// sampled once a minute needed two more minutes.
	MinSampleInterval = 10 * time.Second
	// ProbeRateCap is the most requests per second all continuous probes
	// together should send when the application's own request rate is not
	// measurable. Meeting the floor for many scenarios can exceed it; the
	// prober then says so (ProbeLoad) instead of starving a scenario.
	ProbeRateCap = 3.0
)

// SamplingPlan is how a workload's iterations are spread over its scenarios.
type SamplingPlan struct {
	// Rate is the iterations per second: the workload's rate, or the rate its
	// scenarios' floor needs when that is higher.
	Rate float64
	// Schedule is one cycle of scenario IDs, repeated by iteration number.
	// len(Schedule) over Rate is the cycle's duration, at most
	// MinSampleInterval for a health probe.
	Schedule []string
	// Raised reports that the floor needed more than the workload's own rate.
	Raised bool
}

// SamplingPlan builds the workload's plan. A health probe samples every
// scenario at least once per MinSampleInterval regardless of weight: each cycle
// starts with one iteration of every scenario, and the iterations the workload's
// rate leaves over in that interval are shared by weight. When the rate cannot
// cover every scenario once per interval it is raised to do so. Other
// workloads (bursts, journeys) follow their weights only.
func (w Workload) SamplingPlan() SamplingPlan {
	scenarios := w.Scenarios
	if len(scenarios) == 0 {
		return SamplingPlan{Rate: w.RatePerSecond}
	}
	if w.Purpose != PurposeHealthProbe {
		total := 0
		for _, scenario := range scenarios {
			total += scenario.Weight
		}
		return SamplingPlan{Rate: w.RatePerSecond, Schedule: weightedCycle(scenarios, total)}
	}
	slots := int(math.Floor(w.RatePerSecond*MinSampleInterval.Seconds() + 1e-9))
	plan := SamplingPlan{Rate: w.RatePerSecond}
	if slots < len(scenarios) {
		slots = len(scenarios)
		plan.Rate = float64(len(scenarios)) / MinSampleInterval.Seconds()
		plan.Raised = true
	}
	for _, scenario := range scenarios {
		plan.Schedule = append(plan.Schedule, scenario.ID)
	}
	plan.Schedule = append(plan.Schedule, weightedCycle(scenarios, slots-len(scenarios))...)
	return plan
}

// weightedCycle is count picks of smooth weighted round-robin: scenarios are
// chosen in proportion to their weights, evenly spread.
func weightedCycle(scenarios []WorkloadScenario, count int) []string {
	total := 0
	for _, scenario := range scenarios {
		total += scenario.Weight
	}
	if total <= 0 {
		return nil
	}
	current := make([]int, len(scenarios))
	picks := make([]string, 0, count)
	for len(picks) < count {
		best := 0
		for index, scenario := range scenarios {
			current[index] += scenario.Weight
			if current[index] > current[best] {
				best = index
			}
		}
		current[best] -= total
		picks = append(picks, scenarios[best].ID)
	}
	return picks
}

// ProbeLoad is the request rate the continuous probes send together.
type ProbeLoad struct {
	RequestsPerSecond float64
	Cap               float64
	// Raised names the workloads whose rate was raised to meet the floor.
	Raised []string
}

// OverCap reports that the probes send more than the cap.
func (l ProbeLoad) OverCap() bool { return l.RequestsPerSecond > l.Cap }

// MeasureProbeLoad sums the request rate of the health-probe workloads: each
// plan's rate times the requests per iteration of the scenarios it samples.
// requestsPerIteration maps a scenario ID to its request count; unknown
// scenarios count as one.
func MeasureProbeLoad(workloads []Workload, requestsPerIteration map[string]int) ProbeLoad {
	load := ProbeLoad{Cap: ProbeRateCap}
	for _, workload := range workloads {
		if workload.Purpose != PurposeHealthProbe {
			continue
		}
		plan := workload.SamplingPlan()
		if len(plan.Schedule) == 0 {
			continue
		}
		requests := 0
		for _, id := range plan.Schedule {
			requests += max(requestsPerIteration[id], 1)
		}
		load.RequestsPerSecond += plan.Rate * float64(requests) / float64(len(plan.Schedule))
		if plan.Raised {
			load.Raised = append(load.Raised, workload.Name)
		}
	}
	return load
}
