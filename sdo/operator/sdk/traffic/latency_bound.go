package traffic

import (
	"fmt"
	"sort"
)

// DefaultLatencyScenarios returns, sorted, the scenarios of health-probe
// workloads whose author set no maxLatency (LatencyBoundAuthored). Until SDO
// calibrates one after install, they keep DefaultMaxLatency, which is
// generous so that any application starts green; a saturated service often
// slows its path many times over long before it fails, and a read that
// normally takes tens of milliseconds can stay under the default while its
// service is pinned at its CPU limit.
func DefaultLatencyScenarios(workloads []Workload) []string {
	seen := map[string]bool{}
	var ids []string
	for _, workload := range workloads {
		if workload.Purpose != PurposeHealthProbe {
			continue
		}
		for _, selected := range workload.Scenarios {
			if seen[selected.ID] || workload.LatencyBoundAuthored(selected.ID) {
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
	return fmt.Sprintf("health-probe scenario %s sets no maxLatency: an install's onboarding soak calibrates it to %d times "+
		"its slowest healthy run, at least %s; without a soak the default p%d bound of %s applies", id,
		LatencyCalibrationFactor, MinCalibratedLatency, DefaultLatencyPercentile, DefaultMaxLatency)
}
