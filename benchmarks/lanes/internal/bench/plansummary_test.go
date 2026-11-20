package bench

import (
	"strings"
	"testing"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/plan"
)

// A node the operator skipped is not a failure: the one-line summary counts it apart, as `plan status` does.
func TestPlanSummaryLineCountsSkippedApartFromFailed(t *testing.T) {
	s := &plan.Status{Arms: []plan.ArmStatus{{Arm: "p1", Done: 1, Skipped: 3}, {Arm: "cold", Failed: 1, Parked: 2}}}
	got := planSummaryLine("x", s)
	for _, want := range []string{"failed 1,", "skipped 3,", "parked 2;"} {
		if !strings.Contains(got, want) {
			t.Fatalf("%q lacks %q", got, want)
		}
	}
}
