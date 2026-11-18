package traffic

import (
	"testing"
	"time"
)

// A horizon that is shorter than the real time to a raise makes a working
// probe look broken, so it must cover the verdict the SLO machinery gives for
// a component that stops answering at the worst moment of the sampling cycle.
func TestDetectionHorizonCoversTheTimeToAJudgedFailure(t *testing.T) {
	for _, tc := range []struct {
		name          string
		timeout       time.Duration
		rate          float64
		scenarioCount int
	}{
		{"slow requests", 10 * time.Second, 0.2, 2},
		{"default timeout", 2 * time.Second, 0.5, 5},
		{"many scenarios", 10 * time.Second, 0.5, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workload := Workload{Name: "w", Purpose: PurposeHealthProbe, RatePerSecond: tc.rate, Timeout: Duration(tc.timeout)}
			for i := 0; i < tc.scenarioCount; i++ {
				workload.Scenarios = append(workload.Scenarios, WorkloadScenario{ID: string(rune('a' + i)), Weight: 1})
			}
			workload = workload.WithDefaults()
			arrival := workload.ScenarioArrival("a")
			slo := workload.ScenarioSLO("a")
			base := time.Unix(1000, 0)
			var worst time.Duration
			for offset := time.Duration(0); offset < arrival; offset += time.Second {
				var samples []Sample
				for i := -5; i < 0; i++ {
					samples = append(samples, Sample{At: base.Add(time.Duration(i) * arrival), Outcome: OutcomeOK})
				}
				for k := 0; k < 40; k++ {
					done := base.Add(offset + time.Duration(k)*arrival + tc.timeout)
					samples = append(samples, Sample{At: done, Outcome: OutcomeTimeout})
					if v := Evaluate("a", slo, samples, done); !v.Healthy && v.FailureSpan >= DefaultHealthMinDuration {
						worst = max(worst, done.Sub(base))
						break
					}
				}
			}
			if horizon := workload.DetectionHorizon(); horizon < worst {
				t.Fatalf("horizon %s is shorter than the %s a judged failure can take", horizon, worst)
			}
		})
	}
	if (Workload{Purpose: PurposeVerifyBurst}).DetectionHorizon() != 0 {
		t.Fatal("only a health probe has a detection horizon")
	}
}
