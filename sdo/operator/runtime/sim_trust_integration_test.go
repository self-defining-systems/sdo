//go:build integration

package runtime

import (
	"fmt"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// TestSimProgramTrustFollowsOnlyVerifiedRepairs sweeps random outcome histories:
// trust accrues only from verified repairs at one revision. Questions preserve
// prior credit; counters never authorize an automatic repair.
func TestSimProgramTrustFollowsOnlyVerifiedRepairs(t *testing.T) {
	sweepSimSeeds(t, func(t *testing.T, seed uint64) {
		rng := newSimRNG(seed)
		revisions := []string{"tree-a", "tree-b", "tree-c"}
		model := &simTrustModel{failures: map[string]int{}}
		var outcomes []*contracts.OutcomeRecord
		var history []string
		revision := revisions[0]
		for step := 0; step < 3+rng.IntN(14); step++ {
			event := simTrustEvent(rng.IntN(int(simEdited) + 1))
			if rng.IntN(5) == 0 {
				revision = revisions[rng.IntN(len(revisions))]
			}
			other := revisions[rng.IntN(len(revisions))]
			if event == simInterruptedElsewhere && other == model.revision {
				event = simInterrupted
			}
			if (event == simInterrupted || event == simInterruptedElsewhere) && model.revision == "" {
				event = simVerified
			}
			// Interrupted signals carry no run; the loop's own revision applies.
			outcomes = append(outcomes, simTrustOutcome(t, event, revision, other))
			model.apply(event, revision, other)
			history = append(history, fmt.Sprintf("%d@%s", event, revision))
			got := ProgramTrustStats(outcomes)["targetport"]
			want := model.state()
			if got != want {
				t.Fatalf("history %v: counters %+v, model %+v", history, got, want)
			}
		}
	})
}

