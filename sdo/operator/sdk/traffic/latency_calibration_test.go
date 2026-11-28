package traffic

import (
	"testing"
	"time"
)

// A calibrated bound is three times the slowest healthy run, at least 1s,
// rounded up to 100ms and never past the iteration deadline.
func TestCalibratedMaxLatencyIsThreeTimesTheSlowestHealthyRun(t *testing.T) {
	for _, c := range []struct{ slowest, deadline, want time.Duration }{
		{2740 * time.Millisecond, 20 * time.Second, 8300 * time.Millisecond},
		{120 * time.Millisecond, 20 * time.Second, time.Second},
		{9 * time.Second, 20 * time.Second, 20 * time.Second},
	} {
		if got := CalibratedMaxLatency(c.slowest, c.deadline); got != c.want {
			t.Fatalf("CalibratedMaxLatency(%s, %s) = %s, want %s", c.slowest, c.deadline, got, c.want)
		}
	}
}

// Only a bound the author wrote (on the scenario or its workload) counts as
// authored; a calibrated one is set on the scenario and leaves others alone.
func TestScenarioLatencyBoundAuthoredAndCalibrated(t *testing.T) {
	w := Workload{Name: "probe-call", Purpose: PurposeHealthProbe, Scenarios: []WorkloadScenario{
		{ID: "free"}, {ID: "bounded", SLO: &SLO{MaxLatency: Duration(2 * time.Second)}}, {ID: "windowed", SLO: &SLO{Window: 4}},
	}}.WithDefaults()
	if w.LatencyBoundAuthored("free") || !w.LatencyBoundAuthored("bounded") || w.LatencyBoundAuthored("windowed") {
		t.Fatal("authored bounds misread")
	}
	got := w.WithScenarioMaxLatency("free", 8*time.Second).WithScenarioMaxLatency("windowed", 3*time.Second)
	if got.ScenarioSLO("free").MaxLatency.Duration() != 8*time.Second || got.ScenarioSLO("windowed").MaxLatency.Duration() != 3*time.Second ||
		got.ScenarioSLO("windowed").Window != 4 || got.ScenarioSLO("bounded").MaxLatency.Duration() != 2*time.Second {
		t.Fatalf("calibrated workload %+v", got.Scenarios)
	}
	if w.ScenarioSLO("free").MaxLatency.Duration() != DefaultMaxLatency || w.Scenarios[2].SLO.MaxLatency != 0 {
		t.Fatal("calibration changed the original workload")
	}
	if (Workload{SLO: SLO{MaxLatency: Duration(time.Second)}, Scenarios: []WorkloadScenario{{ID: "free"}}}).LatencyBoundAuthored("free") != true {
		t.Fatal("a workload-wide bound is authored for each of its scenarios")
	}
}
