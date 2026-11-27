package journeycheck

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// ItemSoak is the healthy run of a scenario at its scheduled cadence.
const ItemSoak = "healthy-run"

// SoakWorkloads keeps the scenarios that passed this run's one-shot checks.
// Drop their peers' share of the rate so a retry does not drive the selected
// probes at the full workload's rate.
func SoakWorkloads(workloads []traffic.Workload, report Report) []traffic.Workload {
	var out []traffic.Workload
	for _, w := range workloads {
		if w.Purpose != traffic.PurposeHealthProbe {
			continue
		}
		var scenarios []traffic.WorkloadScenario
		keep := map[string]bool{}
		for _, scenario := range w.Scenarios {
			if scenario.KnownBroken == nil && report.judged(scenario.ID) && report.redOf(scenario.ID) == "" {
				scenarios = append(scenarios, scenario)
				keep[scenario.ID] = true
			}
		}
		if len(scenarios) == 0 {
			continue
		}
		if len(scenarios) != len(w.Scenarios) {
			plan := w.WithDefaults().SamplingPlan()
			slots := 0
			for _, id := range plan.Schedule {
				if keep[id] {
					slots++
				}
			}
			w.RatePerSecond = plan.Rate * float64(slots) / float64(len(plan.Schedule))
			w.Scenarios = scenarios
		}
		out = append(out, w)
	}
	return out
}

// Soaked judges the windows a prober recorded over a healthy run: a scenario
// with a failed sample, or that never qualified, is red.
func Soaked(windows map[string]traffic.Window) []Result {
	var out []Result
	for name, window := range windows {
		for _, o := range window.Scenarios {
			id := o.Scenario.ID
			failed, worst := 0, ""
			budget := window.Workload.ScenarioSLO(id).MaxLatency.Duration()
			samples := slices.Concat(o.Samples, o.Confirming)
			for _, s := range samples {
				switch {
				case s.Outcome != traffic.OutcomeOK:
					worst = describe(s)
				case s.CleanupError != "":
					worst = "cleanup: " + s.CleanupError
				case budget > 0 && s.Latency > budget:
					worst = fmt.Sprintf("iteration latency %s > %s", s.Latency.Round(time.Millisecond), budget)
				default:
					continue
				}
				failed++
			}
			r := Result{ItemSoak, name + "/" + id, Green, fmt.Sprintf("%d samples, none failed", len(o.Samples))}
			first, last, listed := markedSpan(o.Samples)
			switch {
			case failed > 0:
				r.Status, r.Detail = Red, fmt.Sprintf("%d of %d samples failed while healthy: %s", failed, len(samples), worst)
			case !o.Qualified:
				r.Status, r.Detail = Red, strings.TrimSpace("never qualified: too few usable samples in the run "+o.LastInvalid)
			case listed && last-first > maxMarkedGrowth:
				r.Status, r.Detail = Red, fmt.Sprintf("probe-marked records in the list users read rose from %d to %d over the run: the probes leave data behind (a delete the application only scheduled?) and it crowds users' records; make cleanup remove it for good and read it back gone", first, last)
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Subject < out[j].Subject })
	return out
}

// maxMarkedGrowth is how many more probe-marked records a users' list may show
// at the end of a healthy run than at its start: the objects of iterations
// still in flight from two probers.
const maxMarkedGrowth = 2

// markedSpan is the probe-marked record count of the first and the last
// sample that read a users' list, in time order.
func markedSpan(samples []traffic.Sample) (first, last int, listed bool) {
	for _, s := range samples {
		if s.Outcome != traffic.OutcomeOK || s.ExistingRecords == "" {
			continue
		}
		if !listed {
			first, listed = s.ExistingMarked, true
		}
		last = s.ExistingMarked
	}
	return first, last, listed
}
