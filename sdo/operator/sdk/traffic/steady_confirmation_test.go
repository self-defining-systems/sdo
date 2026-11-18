package traffic

import (
	"fmt"
	"testing"
	"time"
)

// A healthy health probe stays evaluable at every instant, not only between
// iterations. A sample is stamped when its iteration starts and becomes
// visible only when the iteration ends, so a window sized to exactly
// minSamples intervals falls one sample short for the length of every
// running iteration of a once-per-interval scenario. The closure recheck then
// reads the probe as unconfirmed every interval, and a stability window longer
// than the interval can never hold.
func TestHealthyProbeStaysEvaluableWhileTheNextIterationRuns(t *testing.T) {
	for _, tc := range []struct {
		rate     float64
		interval string
	}{
		{rate: 0.3},
		{rate: 0.2},
		{rate: 0.1, interval: "30s"},
	} {
		t.Run(fmt.Sprintf("%v/%s", tc.rate, tc.interval), func(t *testing.T) {
			text := fmt.Sprintf(`{"apiVersion":%q,"kind":%q,"name":"probe-write","purpose":"health-probe","ratePerSecond":%v,"iterationTimeout":"30s",`,
				APIVersion, WorkloadKind, tc.rate)
			if tc.interval != "" {
				text += fmt.Sprintf(`"sampleInterval":%q,`, tc.interval)
			}
			text += `"scenarios":[{"id":"write-issue"},{"id":"write-commit"}]}`
			workload, err := ParseWorkloadJSON([]byte(text))
			if err != nil {
				t.Fatal(err)
			}
			plan := workload.SamplingPlan()
			interval := workload.EffectiveSampleInterval()
			// An iteration that runs for a tenth of the interval, well inside its SLO.
			running := interval / 10
			horizon := 5 * time.Minute
			start := time.Unix(0, 0)
			for id, times := range scenarioTimes(plan, horizon) {
				slo := workload.ScenarioSLO(id)
				warm := time.Duration(slo.MinSamples+1) * interval
				for now := warm; now < horizon; now += 250 * time.Millisecond {
					var visible []Sample
					for _, at := range times {
						if at+running <= now {
							visible = append(visible, Sample{At: start.Add(at), Outcome: OutcomeOK, Latency: running})
						}
					}
					if !Evaluate(id, slo, visible, start.Add(now)).Evaluated {
						t.Fatalf("healthy scenario %s is not evaluable at %s: %d visible samples within maxAge %s",
							id, now, len(visible), slo.MaxAge.Duration())
					}
				}
			}
		})
	}
}
