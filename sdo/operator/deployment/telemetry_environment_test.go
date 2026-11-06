package deployment

import (
	"slices"
	"testing"
)

func TestDetectorProbePassesTheTelemetryEnvironmentNotAURL(t *testing.T) {
	args := AcceptedDetectorProbe{TelemetryEnvironment: "/sdo/telemetry/environment.json"}.evaluationArgs("/accepted", "shop")
	i := slices.Index(args, "--telemetry-environment")
	if i < 0 || args[i+1] != "/sdo/telemetry/environment.json" {
		t.Fatalf("args = %v", args)
	}
	if slices.Contains(args, "--metrics-url") {
		t.Fatalf("args carry the retired --metrics-url: %v", args)
	}
	if plain := (AcceptedDetectorProbe{}).evaluationArgs("/accepted", "shop"); slices.Contains(plain, "--telemetry-environment") {
		t.Fatalf("a probe without an environment passes none: %v", plain)
	}
}
