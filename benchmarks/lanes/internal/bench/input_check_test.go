//go:build integration

package bench

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// The subject checks its own inputs (`check-input`, docs/contract.md); lanes passes the kind and the path and takes the
// answer. When the subject answers, the platform runs no check of its own.

func inputSpec(base string) Spec {
	return Spec{Name: "e", Namespace: "app1", Operator: Operator{Hooks: "test", Revision: "4d2915b3f", Baseline: base}}
}

func TestSubjectChecksItsInputs(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	st := t.TempDir()
	t.Setenv("HOOK_STATE", st)
	base := filepath.Join(f.specDir, "base")
	s := inputSpec(base)

	must(t, os.WriteFile(filepath.Join(st, "check-input"), []byte(`{"ok":false,"detail":"bundle head 1234 is not the baseline commit"}`), 0o644))
	var log bytes.Buffer
	err = checkSpecSubjectInputs(context.Background(), inv, s, NewRenderedChecks(&log))
	if err == nil || !strings.Contains(err.Error(), "bundle head 1234") || !strings.Contains(err.Error(), "operator.baseline") {
		t.Fatalf("a refusal by the subject must refuse the spec naming the input, got %v", err)
	}
	env, _ := os.ReadFile(filepath.Join(st, "check-input.env"))
	for _, want := range []string{"BENCH_INPUT_KIND=baseline", "BENCH_INPUT_PATH=" + base, "BENCH_INPUT_PHASE=add"} {
		if !strings.Contains(string(env), want) {
			t.Errorf("the hook was not told %s; it saw %q", want, env)
		}
	}

	must(t, os.WriteFile(filepath.Join(st, "check-input"), []byte(`{"ok":true,"detail":"clean"}`), 0o644))
	if err := checkSpecSubjectInputs(context.Background(), inv, s, NewRenderedChecks(&log)); err != nil {
		t.Fatalf("an accepted input: %v", err)
	}
}

func TestPreContractPinKeepsTheBuiltInInputCheckAndSaysSoOnce(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	t.Setenv("HOOK_STATE", t.TempDir()) // the test hook answers 64 (unsupported) for check-input
	var log bytes.Buffer
	c := NewRenderedChecks(&log)
	s := inputSpec(filepath.Join(f.specDir, "base")) // a baseline.json with no bundle: the built-in check cannot judge it
	for i := 0; i < 3; i++ {
		must(t, checkSpecSubjectInputs(context.Background(), inv, s, c))
	}
	if n := strings.Count(log.String(), "pre-contract pin"); n != 1 {
		t.Errorf("want one pre-contract note, got %d in %q", n, log.String())
	}
}

// A subject that answers `preflight` owns its checks: the platform runs no baseline or memory check for its specs.
func TestPreflightRunsNoPlatformInputCheckWhenTheSubjectAnswersPreflight(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	st := t.TempDir()
	t.Setenv("HOOK_STATE", st)
	must(t, os.WriteFile(filepath.Join(st, "preflight"), []byte(`{"checks":[{"check":"mine","ok":true,"detail":"fine"}]}`), 0o644))
	base := filepath.Join(f.specDir, "base")
	specs := []specFile{{Spec: inputSpec(base), dir: f.specDir}}
	var ran []string
	run := func(ctx context.Context, host, script string) (string, error) {
		ran = append(ran, script)
		if strings.Contains(script, " preflight 2>/dev/null") { // the probe of the subject's own preflight verb
			ans := `{"checks":[{"check":"mine","ok":true,"detail":"fine"}]}`
			return "RC 0\nFIRST " + ans + "\nLAST " + ans + "\n", nil
		}
		return "", nil
	}
	var mu sync.Mutex
	var results []string
	add := func(order int, check, host string, ok bool, f string, a ...any) {
		mu.Lock()
		results = append(results, check)
		mu.Unlock()
	}
	var wg sync.WaitGroup
	newHookSubject().Local(context.Background(), inv, specs, PreflightOptions{}, run, add, &wg)
	wg.Wait()
	for _, s := range ran {
		if strings.Contains(s, "memory migrate") || strings.Contains(s, "sdo-operator") {
			t.Errorf("the platform ran a subject check itself: %s", s)
		}
	}
	for _, r := range results {
		if strings.HasPrefix(r, "memory") {
			t.Errorf("platform memory result %q for a subject that answers preflight", r)
		}
	}
}
