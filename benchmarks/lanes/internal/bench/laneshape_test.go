//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// A lane's allocation is part of its identity: a build for a spec whose lane shape differs from what the ledger holds
// for the lane (a lane reassigned to a pool with other cpus or memory) rebuilds the cluster with the new shape. It is
// never reused with the old cores and memory.
func TestBuildRebuildsALaneWhoseShapeChanged(t *testing.T) {
	r := limitRunner(t, 3) // the new shape: 3 cores, 2Gi
	a, _ := r.allocator()
	if _, err := a.Take("lane-a", 2, 1<<30); err != nil { // the old shape: 2 cores, 1Gi
		t.Fatal(err)
	}
	if err := a.MarkCreated("lane-a"); err != nil {
		t.Fatal(err)
	}
	out, down := filepath.Join(t.TempDir(), "env"), filepath.Join(t.TempDir(), "down")
	r.s.ClusterUp, r.s.ClusterDown = printEnv+out, "touch "+down
	r.build(context.Background())
	if b, _ := os.ReadFile(out); string(b) != "2-4|2147483648|eval.slice" {
		t.Fatalf("cluster-up env %q: the lane kept its old shape", b)
	}
	if _, err := os.Stat(down); err != nil {
		t.Fatal("the old cluster was not deleted")
	}
	es, _ := a.Entries()
	if len(es["lane-a"].CPUs) != 3 || es["lane-a"].Memory != 2<<30 {
		t.Fatalf("ledger %+v", es["lane-a"])
	}
	var p Phase
	if err := readJSON(filepath.Join(r.dir, phaseFile), &p); err != nil || p.Detail != "shape changed: rebuilding" {
		t.Fatalf("phase %+v (%v)", p, err)
	}
}

// A lane whose shape is unchanged is reused, not rebuilt.
func TestBuildKeepsALaneWhoseShapeIsUnchanged(t *testing.T) {
	r := limitRunner(t, 3)
	a, _ := r.allocator()
	if _, err := a.Take("lane-a", 3, 2<<30); err != nil {
		t.Fatal(err)
	}
	if r.shapeChanged(context.Background()) {
		t.Fatal("an unchanged lane was rebuilt")
	}
}
