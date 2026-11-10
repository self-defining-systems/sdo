package traffic

import (
	"fmt"
	"slices"
	"strings"
)

// KnownBrokenChanges describes the KnownBroken scenarios of window whose state
// changed from their declaration: one that now meets its SLO, or one whose
// recent failures include a status key outside FailsWith. It is empty when
// every KnownBroken scenario still fails as declared or has too few samples to
// judge. It is a note for the detection designer, never a finding.
func KnownBrokenChanges(window Window) string {
	var notes []string
	for _, observed := range window.Scenarios {
		id := observed.Scenario.ID
		declared := window.Workload.KnownBrokenScenario(id)
		if declared == nil {
			continue
		}
		verdict := Evaluate(id, window.Workload.ScenarioSLO(id), observed.Samples, window.ObservedAt)
		if !verdict.Evaluated {
			continue
		}
		if verdict.Healthy {
			notes = append(notes, fmt.Sprintf("known-broken scenario %s meets its SLO now (declared: %s); remove knownBroken so it gates health", id, declared.Reason))
			continue
		}
		var different []string
		for _, sample := range verdict.RecentFailures {
			if key := statusKey(sample); !slices.Contains(declared.FailsWith, key) && !slices.Contains(different, key) {
				different = append(different, key)
			}
		}
		if len(different) > 0 {
			slices.Sort(different)
			notes = append(notes, fmt.Sprintf("known-broken scenario %s fails differently: %s (declared failsWith %s: %s)",
				id, strings.Join(different, ", "), strings.Join(declared.FailsWith, ", "), declared.Reason))
		}
	}
	slices.Sort(notes)
	return strings.Join(notes, "; ")
}
