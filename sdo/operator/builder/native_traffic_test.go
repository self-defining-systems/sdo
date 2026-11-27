package builder

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

func TestStagedWorkloadsCarryTheObjectiveCadence(t *testing.T) {
	dir := t.TempDir()
	const source = "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe\npurpose: health-probe\nscenarios: [{id: read}]\n"
	if err := os.WriteFile(filepath.Join(dir, "probe.yaml"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	const target = 45 * time.Second
	documents, err := loadTrafficWorkloads(dir, target)
	if err != nil {
		t.Fatal(err)
	}
	// The controller, validator and journey runner all parse the staged JSON.
	probe, err := traffic.ParseWorkloadJSON(documents["probe"])
	if err != nil {
		t.Fatal(err)
	}
	if probe.EffectiveSampleInterval() <= traffic.MinSampleInterval || probe.ExpectedDetection("read", 0).Worst > target {
		t.Fatalf("staged probe misses the target default: %+v", probe)
	}
}
