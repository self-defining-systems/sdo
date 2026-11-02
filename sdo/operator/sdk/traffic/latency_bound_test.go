//go:build integration

package traffic_test

import (
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// slowSamples is a path whose service is CPU-saturated: every request still
// succeeds, but takes about fifty times its healthy latency (a 7 ms median
// read measured 350 ms on a CPU-throttled astronomy ad service).
func slowSamples(now time.Time, latency time.Duration) []traffic.Sample {
	samples := make([]traffic.Sample, 0, traffic.DefaultWindow)
	for i := traffic.DefaultWindow; i > 0; i-- {
		samples = append(samples, traffic.Sample{At: now.Add(-time.Duration(i) * time.Second), Outcome: traffic.OutcomeOK, Latency: latency})
	}
	return samples
}

func TestDefaultLatencyBoundMissesASaturatedPathThatAMedianBoundCatches(t *testing.T) {
	now := time.Now()
	saturated := slowSamples(now, 350*time.Millisecond)
	defaults := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "ads"})
	if verdict := traffic.Evaluate("ads", defaults.ScenarioSLO("ads"), saturated, now); !verdict.Healthy {
		t.Fatalf("the default bound is expected to stay green here (the gap this note exists for): %+v", verdict)
	}
	bounded := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "ads",
		SLO: &traffic.SLO{LatencyPercentile: 50, MaxLatency: traffic.Duration(150 * time.Millisecond)}})
	slo := bounded.ScenarioSLO("ads")
	if verdict := traffic.Evaluate("ads", slo, saturated, now); verdict.Healthy {
		t.Fatalf("a median bound near healthy latency must fire on a 50x slowdown: %+v", verdict)
	}
	healthy := slowSamples(now, 7*time.Millisecond)
	if verdict := traffic.Evaluate("ads", slo, healthy, now); !verdict.Healthy {
		t.Fatalf("healthy latency must stay green under the bound: %+v", verdict)
	}
	// One pause in an otherwise healthy window is the slowest sample, so it
	// would breach a p90 bound of five samples; the median ignores it.
	healthy[2].Latency = 1200 * time.Millisecond
	if verdict := traffic.Evaluate("ads", slo, healthy, now); !verdict.Healthy {
		t.Fatalf("a single pause must not breach a median bound: %+v", verdict)
	}
}

func TestDefaultLatencyScenariosNamesHealthScenariosWithoutTheirOwnBound(t *testing.T) {
	health := workload(t, "health", traffic.PurposeHealthProbe,
		traffic.WorkloadScenario{ID: "browse"},
		traffic.WorkloadScenario{ID: "ads", SLO: &traffic.SLO{MaxLatency: traffic.Duration(300 * time.Millisecond)}})
	burst := workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "checkout"})
	got := traffic.DefaultLatencyScenarios([]traffic.Workload{health, burst})
	if len(got) != 1 || got[0] != "browse" {
		t.Fatalf("only the continuously probed scenario on the default bound is noted: %v", got)
	}
	if note := traffic.DefaultLatencyNote(got[0]); !strings.Contains(note, "browse") || !strings.Contains(note, "maxLatency") {
		t.Fatalf("note must name the scenario and the setting: %s", note)
	}
	// A workload-level bound covers every scenario that does not override it.
	whole := health
	whole.SLO.MaxLatency = traffic.Duration(400 * time.Millisecond)
	if got := traffic.DefaultLatencyScenarios([]traffic.Workload{whole}); len(got) != 0 {
		t.Fatalf("a workload bound applies to its scenarios: %v", got)
	}
}
