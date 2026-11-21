//go:build integration

package bench

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The first manager pass after `plan add` took about five minutes: every node's submit re-asked the hooks checkout
// (describe, footprint) and each host (pin check) from scratch, one node and one host after another. A session's
// answers are shared, and the hosts are asked together within a bound.

func fourHostInventory(t *testing.T) Inventory {
	t.Helper()
	var b strings.Builder
	b.WriteString("hosts:\n")
	var names []string
	for _, h := range []string{"a", "b", "c", "d", "e", "f"} {
		names = append(names, h)
		b.WriteString("  " + h + ": {ssh: u@" + h + ", staging: /s, kubeconfigs: /k, evidence: /e, max_load5: 10, max_builds: 1, clusters: [" + h + "1]}\n")
	}
	b.WriteString("checkouts:\n  fuzzops: {0b528ac: {")
	for i, h := range names {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(h + ": /fz")
	}
	b.WriteString("}}\n")
	inv, err := loadInv(t, b.String())
	must(t, err)
	return inv
}

func TestPinCheckAsksHostsTogetherWithinABound(t *testing.T) {
	inv := fourHostInventory(t)
	var running, peak atomic.Int32
	old := addHostRunner
	addHostRunner = func(Inventory) runFn {
		return func(_ context.Context, _, _ string) (string, error) {
			n := running.Add(1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(150 * time.Millisecond)
			running.Add(-1)
			return `{"revision":"0b528ac0000","measure_version":12}`, nil
		}
	}
	t.Cleanup(func() { addHostRunner = old })
	s := Spec{Fuzzops: "0b528ac", Hosts: []string{"a", "b", "c", "d", "e", "f"}}
	start := time.Now()
	if err := checkPins(context.Background(), inv, s, nil, newMemoMap[string]()); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("6 hosts took %s: asked one after another", took)
	}
	if p := peak.Load(); p < 2 || p > int32(maxAddProbes) {
		t.Errorf("peak concurrent host probes %d, want between 2 and %d", p, maxAddProbes)
	}
}

// One answer per key however many ask at once: a session shared by concurrent validations runs each probe once.
func TestMemoRunsAProbeOnceForConcurrentAsks(t *testing.T) {
	m := newMemoMap[int]()
	var calls atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := m.do("k", func() (int, error) { calls.Add(1); time.Sleep(50 * time.Millisecond); return 7, nil })
			if v != 7 || err != nil {
				t.Errorf("got %d, %v", v, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("the probe ran %d times", calls.Load())
	}
}

// Submitting the nodes of a plan one by one through one session asks the hooks checkout once, not once per node.
func TestSubmitsThroughOneSessionDescribeTheSubjectOnce(t *testing.T) {
	f := newFixture(t)
	st, log := t.TempDir(), filepath.Join(t.TempDir(), "hook.log")
	t.Setenv("HOOK_STATE", st)
	t.Setenv("HOOK_LOG", log)
	must(t, os.WriteFile(filepath.Join(st, "describe"), []byte(`{"max_startup":"7m"}`), 0o644))
	checks := NewRenderedChecks(nil)
	for _, name := range []string{"n1", "n2", "n3"} {
		body := strings.Replace(exampleSpec, "name: detect-1", "name: "+name, 1)
		if err := Add(context.Background(), f.root, writeSpec(t, f.specDir, body), nil, WithChecks(checks)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	b, _ := os.ReadFile(log)
	if n := strings.Count(string(b), "describe "); n != 1 {
		t.Fatalf("describe ran %d times for 3 submits:\n%s", n, b)
	}
}
