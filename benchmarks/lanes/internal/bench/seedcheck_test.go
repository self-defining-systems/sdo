//go:build integration

package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// seedLab is a flab whose spec has hooks (a checkout of the pin on the local host h1) and a seed directory in the
// baseline's format. The memory check script is replaced by memoryOut, so no sdo-operator is built.
func seedSpec(t *testing.T, l *flab, hooksRev string) (spec []byte, seed string) {
	t.Helper()
	seed = t.TempDir()
	must(t, os.WriteFile(filepath.Join(seed, "baseline.json"), []byte(`{"namespace":"app1","bundle":"b.bundle","branch":"main","commit":"abc1234"}`), 0o644))
	y := fmt.Sprintf(`name: s1
app: examples/shop
fuzzops: 0b528ac
operator: {hooks: test, revision: %s, baseline: %s, seed: %s}
namespace: app1
faults:
  instances: %s
  ids: [a@x]
sequences: [[1]]
run: {window: 1s}
ready_for: 100ms
ready_timeout: 1s
cap: 10m
lanes: 2
hosts: [h1]
`, hooksRev, t.TempDir(), seed, filepath.Join(l.specs, "instances.json"))
	p := filepath.Join(l.specs, "s1.yaml")
	must(t, os.WriteFile(p, []byte(y), 0o644))
	s, err := LoadSpec(p)
	must(t, err)
	b, err := json.Marshal(s)
	must(t, err)
	return b, seed
}

func stubMemoryRunner(t *testing.T, out string, runs *int) {
	old := memoryRunner
	memoryRunner = func(Inventory) runFn {
		return func(context.Context, string, string) (string, error) { *runs++; return out, nil }
	}
	t.Cleanup(func() { memoryRunner = old })
}

// An invalid seed fails at plan add, naming the seed and the check that failed, before any lane is built.
func TestPlanSpecCheckRefusesAnInvalidSeed(t *testing.T) {
	l := newFlab(t, "h1=c1")
	spec, seed := seedSpec(t, l, "abcdef0")
	inv, err := LoadInventory(string(l.root))
	must(t, err)
	runs := 0
	stubMemoryRunner(t, "HEAD abc1234\nCHECK probe sampleInterval 2s is outside [10s, 2m]\nEXIT 1\n", &runs)
	err = planSpecCheck(inv)(spec)
	if err == nil || !strings.Contains(err.Error(), "operator.seed") || !strings.Contains(err.Error(), seed) || !strings.Contains(err.Error(), "sampleInterval") {
		t.Fatalf("plan add check: %v, want a refusal naming the seed and the failed check", err)
	}
	// A clean seed passes, and the same seed is checked once for any number of specs.
	runs = 0
	stubMemoryRunner(t, "HEAD abc1234\nEXIT 0\n", &runs)
	check := planSpecCheck(inv)
	must(t, check(spec))
	must(t, check(spec))
	if runs != 1 {
		t.Fatalf("the memory check ran %d times for one seed", runs)
	}
}

// Without a hooks checkout of the pin on the host that reads the directory the check cannot run: it says so (a note
// for the caller), it does not fail and it runs nothing.
func TestMemoryDirCheckWithoutACheckoutIsNotCheckable(t *testing.T) {
	runs := 0
	run := func(context.Context, string, string) (string, error) { runs++; return "", nil }
	dir := t.TempDir()
	must(t, os.WriteFile(filepath.Join(dir, "baseline.json"), []byte(`{"namespace":"n","bundle":"b","branch":"main","commit":"abc1234"}`), 0o644))
	r := checkMemoryDir(context.Background(), run, "h1", "", "", dir, nil)
	if r.Ran || r.Err != nil || !strings.Contains(r.Note, "not checkable here") || runs != 0 {
		t.Fatalf("%+v runs %d", r, runs)
	}
}
