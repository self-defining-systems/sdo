package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
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
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
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
	if _, err := os.Stat(filepath.Join(repo, ".sdo", "diagnostics", "detectors")); err == nil {
		t.Fatal("the agent's repository was changed")
	}
}
