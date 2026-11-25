package journeycheck

import (
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/sdk/traffic"
)

// ItemSoak is the short run of the whole probe set at its own schedule.
const ItemSoak = "healthy-run"

// Soaked judges the windows a prober recorded over a healthy run: a scenario
// with a failed sample, or that never qualified, is red.
func Soaked(windows map[string]traffic.Window) []Result {
	var out []Result
	for name, window := range windows {
		for _, o := range window.Scenarios {
			id := o.Scenario.ID
			failed, worst := 0, ""
			for _, s := range append(append([]traffic.Sample{}, o.Samples...), o.Confirming...) {
				if s.Outcome != traffic.OutcomeOK {
					failed++
					worst = describe(s)
				}
			}
			r := Result{ItemSoak, name + "/" + id, Green, fmt.Sprintf("%d samples, none failed", len(o.Samples))}
			switch {
			case failed > 0:
				r.Status, r.Detail = Red, fmt.Sprintf("%d of %d samples failed while healthy: %s", failed, len(o.Samples), worst)
			case !o.Qualified:
				r.Status, r.Detail = Red, strings.TrimSpace("never qualified: too few usable samples in the run "+o.LastInvalid)
			}
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Subject < out[j].Subject })
	return out
}
