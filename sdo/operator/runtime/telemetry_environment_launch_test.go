package runtime

import (
	"slices"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestLaunchConfigPassesTheTelemetryEnvironmentFlag(t *testing.T) {
	args := controllerConfigToArgs(&contractsv1alpha1.ControllerConfig{TelemetryEnvironmentFile: "/sdo/telemetry/environment.json"})
	i := slices.Index(args, "--telemetry-environment")
	if i < 0 || args[i+1] != "/sdo/telemetry/environment.json" {
		t.Fatalf("args = %v", args)
	}
	if empty := controllerConfigToArgs(&contractsv1alpha1.ControllerConfig{}); slices.Contains(empty, "--telemetry-environment") {
		t.Fatalf("an unset environment passes no flag: %v", empty)
	}
}

func TestRemoteMemoryCarriesTheHostsTelemetrySources(t *testing.T) {
	sources := []byte("version: 1\nsources: []\n")
	remote, err := NewRemoteMemory(&contractsv1alpha1.ReadyResponse{MemoryCommit: "c", BinaryDigest: "d", TelemetrySources: sources}, unusedMemoryRPC{})
	if err != nil {
		t.Fatal(err)
	}
	if string(remote.TelemetrySources) != string(sources) {
		t.Fatalf("remote memory telemetry sources = %q, want the host's", remote.TelemetrySources)
	}
}
