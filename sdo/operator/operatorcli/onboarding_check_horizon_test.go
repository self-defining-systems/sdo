package operatorcli

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// The break wait follows the memory's own probes: slow requests at a low rate
// need longer than the flag's default to be judged as failing.
func TestProbeDetectionHorizonFollowsTheRepositoryWorkloads(t *testing.T) {
	repo := t.TempDir()
	dir := filepath.Join(repo, ".sdo", "diagnostics", "traffic", "workloads")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	workload := "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe-read\npurpose: health-probe\nratePerSecond: 0.2\ntimeout: 10s\niterationTimeout: 30s\nscenarios:\n  - {id: a}\n  - {id: b}\n"
	if err := os.WriteFile(filepath.Join(dir, "probe-read.yaml"), []byte(workload), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := probeDetectionHorizon(repo); got <= time.Minute {
		t.Fatalf("horizon %s: ten-second requests sampled every ten seconds cannot be judged within the default minute", got)
	}
	if got := probeDetectionHorizon(t.TempDir()); got != 0 {
		t.Fatalf("a repository without probes has horizon %s", got)
	}
}

func TestProbeDetectionHorizonUsesTheHealthObjectiveTarget(t *testing.T) {
	repo := t.TempDir()
	workloads := filepath.Join(repo, ".sdo", "diagnostics", "traffic", "workloads")
	if err := os.MkdirAll(workloads, 0o755); err != nil {
		t.Fatal(err)
	}
	data := []byte("apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe\npurpose: health-probe\nscenarios: [{id: read}]\n")
	if err := os.WriteFile(filepath.Join(workloads, "probe.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	writeSDO(t, repo, "goal.md", "---\nschema_version: 1\n---\n# Health objective\n\nStay healthy.\n")
	defaultHorizon := probeDetectionHorizon(repo)
	defaultProbe, err := traffic.ParseWorkloadJSONWithDetectionTarget([]byte(`{
		"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"probe",
		"purpose":"health-probe","scenarios":[{"id":"read"}]
	}`), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if want := defaultProbe.DetectionHorizon(); defaultHorizon != want {
		t.Fatalf("goal with no target: got horizon %s, want %s", defaultHorizon, want)
	}
	writeSDO(t, repo, "goal.md", "---\nschema_version: 1\n---\n# Health objective\n\nDetection target: 60s\n")
	if got := probeDetectionHorizon(repo); got <= defaultHorizon {
		t.Fatalf("goal target did not inform the horizon: %s, default %s", got, defaultHorizon)
	}
}
