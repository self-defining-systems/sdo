package bench

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// A down lane's cluster is deleted by a release that names no workload checkout (the lane never got a build to
// prepare one). That step must not ask a workload for its version: it only runs the host's cluster-down.
func TestReleaseOfALaneWithNoWorkloadCheckoutRunsClusterDown(t *testing.T) {
	steps := filepath.Join(t.TempDir(), "vc1.steps")
	dir := filepath.Join(steps, "release-1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "down")
	s := Step{ID: "release-1", Kind: StepRelease, Lane: "vc1", Host: "h", LaneDir: t.TempDir(),
		ClusterDown: "touch " + marker, DeleteCluster: true}
	b, _ := json.Marshal(s)
	if err := os.WriteFile(filepath.Join(dir, "step.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RunStep(context.Background(), filepath.Join(dir, "step.json")); err != nil {
		t.Fatal(err)
	}
	var res Result
	rb, err := os.ReadFile(filepath.Join(dir, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(rb, &res)
	if !res.OK || !res.Gone {
		t.Fatalf("release did not delete the cluster: %+v", res)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("cluster-down did not run")
	}
}
