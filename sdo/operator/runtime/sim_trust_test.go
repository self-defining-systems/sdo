package runtime

import (
	"fmt"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// simTrustEvent is one step of a program's outcome history.
type simTrustEvent int

const (
	// simVerified: a successful incident whose apply response shows the bound
	// post-apply diagnosis clear, at the current revision.
	simVerified simTrustEvent = iota
	// simUnverified: the program applied, but the incident did not succeed.
	simUnverified
	// simInconclusive: the incident succeeded but its tool record has no
	// post-apply evidence (truncated or missing).
	simInconclusive
	// simInterrupted: a fast-path attempt was cut off before it could finish;
	// only its failure signal remains.
	simInterrupted
	// simInterruptedElsewhere: the same signal bound to another revision.
	simInterruptedElsewhere
	// simEdited: a run of the program with no application; at a new revision it
	// preserves the streak, as does the current revision.
	simEdited
)

// simTrustModel restates the trust rules independently of ProgramTrustStats:
// Verified successes form a streak at one revision. Failures reset it; questions preserve prior credit without extending it.
// Failures are remembered per revision and prevent trust at that revision.
type simTrustModel struct {
	revision string
	streak   int
	failures map[string]int
}

func (m *simTrustModel) enter(revision string) {
	if revision != m.revision {
		m.revision = revision
	}
}

func (m *simTrustModel) apply(event simTrustEvent, revision, other string) {
	switch event {
	case simVerified:
		m.enter(revision)
		m.streak++
	case simUnverified:
		m.enter(revision)
		m.failures[revision]++
		m.streak = 0
	case simInconclusive:
		m.enter(revision)
	case simEdited:
		m.enter(revision)
	case simInterrupted:
		m.failures[m.revision]++
		m.streak = 0
	case simInterruptedElsewhere:
		m.failures[other]++
	}
}

func (m *simTrustModel) state(threshold int) ProgramTrust {
	return ProgramTrust{VerifiedSuccesses: m.streak, Failures: m.failures[m.revision], LastChange: m.revision}
}

func simTrustOutcome(t *testing.T, event simTrustEvent, revision, other string) *contracts.OutcomeRecord {
	t.Helper()
	const id = "targetport"
	switch event {
	case simVerified:
		return verifiedTrustOutcome(t, id, revision)
	case simUnverified:
		outcome := verifiedTrustOutcome(t, id, revision)
		outcome.Classification = "failed"
		return outcome
	case simInconclusive:
		outcome := verifiedTrustOutcome(t, id, revision)
		outcome.ProgramToolCalls = nil
		settleTestOutcome(outcome)
		return outcome
	case simInterrupted:
		return &contracts.OutcomeRecord{SourceCommit: revision, ProgramFailureSignals: []string{"application_verification_failure:" + id}}
	case simInterruptedElsewhere:
		return &contracts.OutcomeRecord{SourceCommit: revision, ProgramFailureSignals: []string{"application_verification_failure:" + id + "@" + other}}
	}
	return &contracts.OutcomeRecord{SourceCommit: revision, ProgramRuns: []*contracts.ProgramRun{{ProgramId: id, ProgramRevision: revision}}}
}

// TestSimProgramTrustFollowsOnlyVerifiedRepairs sweeps random outcome histories:
// trust accrues only from verified repairs at one revision. Questions preserve
// prior credit, and the program is trusted exactly at the threshold.
func TestSimProgramTrustFollowsOnlyVerifiedRepairs(t *testing.T) {
	sweepSimSeeds(t, func(t *testing.T, seed uint64) {
		rng := newSimRNG(seed)
		threshold := 1 + rng.IntN(4)
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
			want := model.state(threshold)
			if got != want {
				t.Fatalf("history %v threshold %d: trust %+v, model %+v", history, threshold, got, want)
			}
		}
	})
}

// TestSimFastPathFiresExactlyAtTrustThreshold: the trusted fast path is
// attempted after the third verified repair and not after the second, and a
// failure afterward closes it again.
