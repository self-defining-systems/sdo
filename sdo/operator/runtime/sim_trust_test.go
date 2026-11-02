package runtime

import (
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// simTrustEvent is one step of a program's outcome history.
type simTrustEvent int

const (
	// simVerified: a successful incident whose apply response shows the bound
	// post-apply diagnosis clear, at the current revision.
	simVerified simTrustEvent = iota
	// simIncidentFailed: the exact application was verified, but the incident did not succeed.
	simIncidentFailed
	// simInconclusive: the incident succeeded but its tool record has no
	// post-apply evidence (truncated or missing).
	simInconclusive
	// simInterrupted: a responder application was cut off before it could finish;
	// only its failure signal remains.
	simInterrupted
	// simInterruptedElsewhere: the same signal bound to another revision.
	simInterruptedElsewhere
	// simApplicationFailed: immutable action evidence proves an execution failure.
	simApplicationFailed
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
	case simVerified, simIncidentFailed:
		m.enter(revision)
		m.streak++
	case simApplicationFailed:
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

func (m *simTrustModel) state() ProgramTrust {
	return ProgramTrust{VerifiedSuccesses: m.streak, Failures: m.failures[m.revision], LastChange: m.revision}
}

func simTrustOutcome(t *testing.T, event simTrustEvent, revision, other string) *contracts.OutcomeRecord {
	t.Helper()
	const id = "targetport"
	switch event {
	case simVerified:
		return verifiedTrustOutcome(t, id, revision)
	case simIncidentFailed:
		outcome := verifiedTrustOutcome(t, id, revision)
		outcome.Classification = "failed"
		return outcome
	case simApplicationFailed:
		outcome := verifiedTrustOutcome(t, id, revision)
		outcome.PlanApplications[0].ActionReports = []*contracts.ActionReport{{FailureReason: "execution_failed"}}
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

