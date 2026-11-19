package traffic

import (
	"fmt"
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
	// MaxSampleInterval bounds a workload's SampleInterval. A failure needs
	// MinSamples samples to be confirmed, so detection takes about
	// MinSamples times the interval and the SLO's MaxAge must span it; 120 s
	// keeps that near six minutes, inside the 10 minute MaxAge ceiling. A
	// probe sampled less often than that confirms outages too late to be a
	// health signal.
	MaxSampleInterval = 120 * time.Second
	// ProbeRateCap is the most requests per second all continuous probes
	// together should send when the application's own request rate is not
	// measurable. Meeting the floor for many scenarios can exceed it; the
	// prober then says so (ProbeLoad) instead of starving a scenario.
	ProbeRateCap = 3.0
)

// EffectiveSampleInterval is how often the workload samples each of its
// scenarios at least: its SampleInterval, else MinSampleInterval.
func (w Workload) EffectiveSampleInterval() time.Duration {
	if w.SampleInterval > 0 {
		return w.SampleInterval.Duration()
	}
	return MinSampleInterval
}

// SamplingPlan is how a workload's iterations are spread over its scenarios.
type SamplingPlan struct {
	// Rate is the iterations per second: the workload's rate, or the rate its
	// scenarios' floor needs when that is higher.
	Rate float64
	// Schedule is one cycle of scenario IDs, repeated by iteration number.
	// len(Schedule) over Rate is the cycle's duration, at most the workload's
	// sample interval for a health probe.
	Schedule []string
	// Raised reports that the floor needed more than the workload's own rate.
	Raised bool
}

// SamplingPlan builds the workload's plan. A health probe samples every
// scenario at least once per sample interval (MinSampleInterval unless the
// workload sets SampleInterval) regardless of weight: each cycle
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
	interval := w.EffectiveSampleInterval()
	slots := int(math.Floor(w.RatePerSecond*interval.Seconds() + 1e-9))
	plan := SamplingPlan{Rate: w.RatePerSecond}
	if slots < len(scenarios) {
		slots = len(scenarios)
		plan.Rate = float64(len(scenarios)) / interval.Seconds()
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

// Message explains an over-cap load and the ways out of it.
func (l ProbeLoad) Message() string {
	return fmt.Sprintf("sampling every scenario at least every %s (the %s floor, or a workload's sampleInterval up to %s) sends %.1f requests/s, above the %.0f/s cap; no scenario was starved: lengthen sampleInterval, or remove or merge scenarios", MinSampleInterval, MinSampleInterval, MaxSampleInterval, l.RequestsPerSecond, l.Cap)
}

// CheckProbeLoad rejects a health-probe set whose sampling sends more than
// ProbeRateCap requests per second, counting each scenario's steps from the
// catalog (a scenario the catalog does not hold counts one). The check is
// deterministic, so the memory check refuses the set before the controller
// would have to run it over the cap.
func CheckProbeLoad(workloads []Workload, catalog Catalog) error {
	requests := make(map[string]int, len(catalog))
	for _, scenario := range catalog {
		requests[scenario.ID] = len(scenario.Steps)
	}
	if load := MeasureProbeLoad(workloads, requests); load.OverCap() {
		return fmt.Errorf("probe load: %s", load.Message())
	}
	return nil
}
