package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// The live items judge the detectors the controller will run: the traffic
// detector of a probe workload exists in the prepared tree although the agent
// only authored the workload, and the agent's repository is left as it was.
func TestPreparedTreeInstallsTheTrafficDetectorOfAProbeWorkload(t *testing.T) {
	repo := t.TempDir()
	workloads := filepath.Join(repo, ".sdo", "diagnostics", "traffic", "workloads")
	if err := os.MkdirAll(workloads, 0o755); err != nil {
		t.Fatal(err)
	}
	workload := "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe-read\npurpose: health-probe\nratePerSecond: 0.2\nscenarios:\n  - {id: a}\n"
	if err := os.WriteFile(filepath.Join(workloads, "probe-read.yaml"), []byte(workload), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "-A"}, {"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"}} {
		if _, err := processio.Git(context.Background(), append([]string{"-C", repo}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	tree, cleanup, err := preparedTree(context.Background(), repo, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	detector := filepath.Join(tree, ".sdo", "diagnostics", "detectors", "health", "traffic-probe-read", "detector.go")
	if _, err := os.Stat(detector); err != nil {
		t.Fatalf("the prepared tree has no traffic detector for the probe workload: %v", err)
	}
	// The check judges what publication installs: the same detector set that
	// validation counts for this tree.
	var got []string
	entries, _ := os.ReadDir(filepath.Join(tree, ".sdo", "diagnostics", "detectors", "health"))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}
	want := memory.PreparedHealthDetectorIDs(memory.HealthOptions{}, map[string][]byte{"probe-read.yaml": []byte(workload)})
	for _, id := range want {
		if !slices.Contains(got, id) {
			t.Fatalf("publication installs %s but the check's tree lacks it (has %v)", id, got)
		}
	}
	if detectorCount(tree) == 0 {
		t.Fatal("detectorCount is zero for a prepared tree")
	}
	if _, err := os.Stat(filepath.Join(repo, ".sdo", "diagnostics", "detectors")); err == nil {
		t.Fatal("the agent's repository was changed")
	}
}
