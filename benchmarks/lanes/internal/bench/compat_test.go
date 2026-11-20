//go:build integration

package bench

import (
	"path/filepath"
	"testing"
)

// A root and a spec written for fuzzops-bench load unchanged: same hosts.yaml, spec, lock and lane-state formats.
// The loaders reject unknown fields, so a renamed or dropped field fails here.
func TestFuzzopsBenchRootAndSpecLoad(t *testing.T) {
	root := Root(filepath.Join("testdata", "compat-root"))
	inv, err := LoadInventory(string(root))
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Hosts) != 2 || inv.Hosts["h1"].Clusters[1] != "c2" || inv.Hosts["h2"].SSH != "h2.example" {
		t.Fatalf("hosts: %+v", inv.Hosts)
	}
	if inv.FuzzopsPath("0b528ac", "h2") == "" || inv.HookCommand["test"] != "./hook.sh" {
		t.Fatalf("checkouts: %+v", inv)
	}
	s, err := LoadSpec(filepath.Join("testdata", "compat-root", "specs", "shop-noop.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Workload != "" || s.Operator.Builtin != "noop" || len(s.Seqs()) != 4 && len(s.Seqs()) != 2 {
		t.Fatalf("spec: %+v", s)
	}
	x, err := root.LoadExperiment("shop-noop")
	if err != nil {
		t.Fatal(err)
	}
	if x.Lock.Spec.Name != "shop-noop" || x.Lock.Spec.Fuzzops != "0b528ac" {
		t.Fatalf("lock: %+v", x.Lock.Spec)
	}
	lanes, err := root.Lanes()
	if err != nil || len(lanes) != 1 || lanes[0].Lane != "c1" || lanes[0].State != LaneFree {
		t.Fatalf("lanes: %+v %v", lanes, err)
	}
}
