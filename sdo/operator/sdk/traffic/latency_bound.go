package traffic

import (
	"fmt"
	"sort"
)

// DefaultLatencyScenarios returns, sorted, the scenarios of health-probe
// workloads whose effective SLO keeps DefaultMaxLatency. The default bound is
// generous so that any application starts green; a saturated service often
// slows its path many times over long before it fails, and a read that
// normally takes tens of milliseconds can stay under the default while its
// service is pinned at its CPU limit. Each scenario returned is a critical
// path whose slowdown health cannot see.
func DefaultLatencyScenarios(workloads []Workload) []string {
	seen := map[string]bool{}
	var ids []string
	for _, workload := range workloads {
		if workload.Purpose != PurposeHealthProbe {
			continue
		}
		for _, selected := range workload.Scenarios {
			if seen[selected.ID] || workload.ScenarioSLO(selected.ID).MaxLatency.Duration() != DefaultMaxLatency {
				continue
			}
			seen[selected.ID] = true
			ids = append(ids, selected.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// DefaultLatencyNote is the non-blocking memory-check note for a scenario
// DefaultLatencyScenarios returns.
func DefaultLatencyNote(id string) string {
	return fmt.Sprintf("health-probe scenario %s keeps the default p%d latency bound of %s: set slo.maxLatency near "+
		"its healthy latency so a slowed or saturated service on its path fires", id, DefaultLatencyPercentile, DefaultMaxLatency)
}
