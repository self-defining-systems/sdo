package traffic

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"sdo.dev/operator/sdk"
)

// Watch is the watch kind the controller runtime emits when synthetic probe
// results may change a scenario's verdict. Traffic detectors declare it so
// they are evaluated as soon as probes fail or recover, not on their interval.
var Watch = sdk.WatchKind{APIVersion: APIVersion, Kind: "SyntheticTraffic"}

// ScenarioObservations are one scenario's recent samples, oldest first.
type ScenarioObservations struct {
	Scenario Descriptor `json:"scenario"`
	Samples  []Sample   `json:"samples"`
	// Qualified reports whether the scenario has succeeded at least once
	// since observation began. A scenario that never succeeded is not a
	// firing health signal: its generator may not match this deployment.
	// It remains unproven and must not establish confirmed health.
	Qualified bool `json:"qualified"`
	// Invalid counts iterations whose requests could not be built or broke a
	// safety rule; LastInvalid explains the latest one.
	Invalid     int    `json:"invalid,omitempty"`
	LastInvalid string `json:"lastInvalid,omitempty"`
}

// Window is what the prober has observed for one workload at ObservedAt.
type Window struct {
	Workload   Workload               `json:"workload"`
	ObservedAt time.Time              `json:"observedAt"`
	Scenarios  []ScenarioObservations `json:"scenarios"`
	// Links holds a link-probe workload's per-edge observations.
	Links []LinkObservations `json:"links,omitempty"`
	// Skipped counts arrivals dropped because MaxInFlight iterations were running.
	Skipped int `json:"skipped,omitempty"`
	// LoadError is set when the workload could not be loaded or run.
	LoadError string `json:"loadError,omitempty"`
}

// Source is implemented by detection contexts that carry synthetic-traffic
// observations. The controller runtime's snapshots implement it.
type Source interface {
	TrafficWindow(workload string) (Window, bool)
}

// WindowFrom returns the observations of workload, when the context carries them.
func WindowFrom(ctx sdk.DetectionContext, workload string) (Window, bool) {
	source, ok := ctx.(Source)
	if !ok {
		return Window{}, false
	}
	return source.TrafficWindow(workload)
}

// SLOVerdict is one scenario's SLO judgement over its recent samples.
type SLOVerdict struct {
	ScenarioID  string
	Evaluated   bool
	Healthy     bool
	Samples     int
	Errors      int
	Timeouts    int
	ErrorRate   float64
	TimeoutRate float64
	Latency     time.Duration
	SLO         SLO
	Violations  []string
	// StatusCounts counts samples by last HTTP status, "dial" for a
	// connection that was refused or reset outright, "transport" for any
	// other failure with no response, and "timeout" for any sample that hit
	// its deadline, whether while dialing or while waiting on a slow
	// response; Sample.DialFailed distinguishes a dial timeout from the
	// rest of the "timeout" bucket.
	StatusCounts   map[string]int
	RecentFailures []Sample
	Span           time.Duration
}

// Evaluate judges samples (oldest first) of one scenario at now. Invalid
// samples and samples older than the SLO's maxAge are ignored, only the latest
// window samples count, and fewer than minSamples leaves the scenario
// unevaluated, which is healthy.
func Evaluate(scenarioID string, slo SLO, samples []Sample, now time.Time) SLOVerdict {
	verdict := SLOVerdict{ScenarioID: scenarioID, Healthy: true, SLO: slo, StatusCounts: map[string]int{}}
	recent := make([]Sample, 0, len(samples))
	for _, sample := range samples {
		if sample.Outcome != OutcomeInvalid && now.Sub(sample.At) <= slo.MaxAge.Duration() {
			recent = append(recent, sample)
		}
	}
	if len(recent) > slo.Window {
		recent = recent[len(recent)-slo.Window:]
	}
	verdict.Samples = len(recent)
	if len(recent) < slo.MinSamples {
		return verdict
	}
	verdict.Evaluated = true
	verdict.Span = recent[len(recent)-1].At.Sub(recent[0].At)
	latencies := make([]time.Duration, 0, len(recent))
	for _, sample := range recent {
		switch sample.Outcome {
		case OutcomeTimeout:
			verdict.Timeouts++
			verdict.StatusCounts["timeout"]++
		case OutcomeError:
			verdict.Errors++
			latencies = append(latencies, sample.Latency)
		default:
			latencies = append(latencies, sample.Latency)
		}
		if sample.Outcome != OutcomeTimeout {
			key := "transport"
			switch {
			case sample.DialFailed:
				key = "dial"
			case sample.Status > 0:
				key = strconv.Itoa(sample.Status)
			}
			verdict.StatusCounts[key]++
		}
		if sample.Outcome != OutcomeOK {
			verdict.RecentFailures = append(verdict.RecentFailures, sample)
		}
	}
	if len(verdict.RecentFailures) > 3 {
		verdict.RecentFailures = verdict.RecentFailures[len(verdict.RecentFailures)-3:]
	}
	total := float64(len(recent))
	verdict.ErrorRate = float64(verdict.Errors+verdict.Timeouts) / total
	verdict.TimeoutRate = float64(verdict.Timeouts) / total
	verdict.Latency = percentile(latencies, slo.LatencyPercentile)
	if verdict.ErrorRate >= slo.MaxErrorRate {
		verdict.Violations = append(verdict.Violations, fmt.Sprintf(
			"error rate %d/%d ≥ %s", verdict.Errors+verdict.Timeouts, len(recent), percent(slo.MaxErrorRate)))
	}
	if verdict.TimeoutRate >= slo.MaxTimeoutRate {
		verdict.Violations = append(verdict.Violations, fmt.Sprintf(
			"timeout rate %d/%d ≥ %s", verdict.Timeouts, len(recent), percent(slo.MaxTimeoutRate)))
	}
	if len(latencies) > 0 && verdict.Latency > slo.MaxLatency.Duration() {
		verdict.Violations = append(verdict.Violations, fmt.Sprintf(
			"p%d latency %s > %s", slo.LatencyPercentile, verdict.Latency.Round(time.Millisecond), slo.MaxLatency.Duration()))
	}
	verdict.Healthy = len(verdict.Violations) == 0
	return verdict
}

// Judge evaluates every qualified scenario of window at its ObservedAt.
func Judge(window Window) []SLOVerdict {
	verdicts := make([]SLOVerdict, 0, len(window.Scenarios))
	for _, observed := range window.Scenarios {
		if !observed.Qualified {
			continue
		}
		id := observed.Scenario.ID
		verdicts = append(verdicts, Evaluate(id, window.Workload.ScenarioSLO(id), observed.Samples, window.ObservedAt))
	}
	return verdicts
}

// percentile uses the nearest-rank method.
func percentile(values []time.Duration, rank int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(left int, right int) bool { return sorted[left] < sorted[right] })
	index := int(math.Ceil(float64(rank)/100*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func percent(value float64) string {
	return strconv.FormatFloat(value*100, 'f', -1, 64) + "%"
}
