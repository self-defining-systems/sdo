package traffic

import (
	"context"
	"strings"
	"testing"
)

// The step log reads like a prober's log: each request, its status, how long
// it took and the start of the answer, in setup, step and cleanup order,
// including SDO's own read-back after a delete.
func TestStepLogShowsEachRequestAndAnswer(t *testing.T) {
	store := &recordStore{records: map[string]string{}}
	engine := inferenceEngine(t, createReadDelete(), store)
	var lines []string
	engine.UseStepLog(func(entry StepLog) { lines = append(lines, entry.String()) })
	if _, sample := engine.Iterate(context.Background(), 1); sample.Outcome != OutcomeOK {
		t.Fatalf("%+v", sample)
	}
	text := strings.Join(lines, "\n")
	for _, want := range []string{"step create: POST /records -> 201", "'id':'101'", "step read: GET /records/101 -> 200", "cleanup delete: DELETE /records/101 -> 204", "cleanup delete-gone: GET /records/101 -> 404"} {
		if !strings.Contains(text, want) {
			t.Fatalf("want %q in:\n%s", want, text)
		}
	}
}
