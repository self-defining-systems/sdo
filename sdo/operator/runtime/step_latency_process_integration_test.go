//go:build integration

package runtime

import (
	"os"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"testing"
	"time"
)

// TestRestartStormStepLatencyReport compares client rate limits. Run it with
// SDO_STEP_LATENCY_REPORT=1 go test -run TestRestartStormStepLatencyReport -v.
func TestRestartStormStepLatencyReport(t *testing.T) {
	if os.Getenv("SDO_STEP_LATENCY_REPORT") == "" {
		t.Skip("set SDO_STEP_LATENCY_REPORT=1 to run the timed restart-storm comparison")
	}
	for _, scenario := range []struct {
		qps       float32
		burst     int
		podEvents int
		spacing   time.Duration
	}{
		{5, 10, 50, 20 * time.Millisecond},
		{50, 100, 50, 20 * time.Millisecond},
		{5, 10, 300, 5 * time.Millisecond},
		{50, 100, 300, 5 * time.Millisecond},
	} {
		result := runRestartStorm(t, lifecycleio.Clock(), scenario.qps, scenario.burst, scenario.podEvents, scenario.spacing)
		t.Logf(
			"qps=%v burst=%d pods=%d every %s: ConfigMap delete evaluated after %s; %d steps, longest %s, %d rate-limited API calls",
			scenario.qps, scenario.burst, scenario.podEvents, scenario.spacing,
			result.detection.Round(100*time.Microsecond), result.steps, result.maxStep.Round(100*time.Microsecond),
			result.apiCalls,
		)
	}
}
