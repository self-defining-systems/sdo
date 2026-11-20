//go:build integration

package bench

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

// Every workload verb has a time limit (hookTimeouts, docs/contract.md): an adapter that stops answering is killed at
// it and the reason names the verb and the limit.
func TestWorkloadVerbPastItsLimitIsKilledWithAReason(t *testing.T) {
	for _, verb := range WorkloadVerbs {
		if hookTimeouts[verb] <= 0 {
			t.Errorf("workload verb %s has no stated limit in hookTimeouts", verb)
		}
	}
	old := hookTimeouts[VerbGate]
	hookTimeouts[VerbGate] = 200 * time.Millisecond
	t.Cleanup(func() { hookTimeouts[VerbGate] = old })
	w := NewWorkload("sleep 5 #", t.TempDir())
	start := time.Now()
	code, _, err := w.Call(context.Background(), WorkloadCall{Verb: VerbGate, Action: "check", Log: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "exceeded its limit") || !strings.Contains(err.Error(), VerbGate) {
		t.Fatalf("verb past its limit: code %d err %v (after %s): want an error naming the verb and its limit", code, err, time.Since(start))
	}
	if time.Since(start) > 3*time.Second {
		t.Fatalf("the verb was not killed at its limit: took %s", time.Since(start))
	}
}
