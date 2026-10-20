package runtime

import (
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

type ProgramTrust struct {
	VerifiedSuccesses int    `json:"verified_successes"`
	Failures          int    `json:"failures"`
	LastChange        string `json:"last_change"`
	Trusted           bool   `json:"trusted"`
}

// ProgramTrustStats derives counters from durable outcomes. A changed
// program revision or any failed application breaks its success streak.
// Corpus replay remains a separate gate; the automatic fast path is off.
func ProgramTrustStats(outcomes []*contracts.OutcomeRecord) map[string]ProgramTrust {
	ordered := append([]*contracts.OutcomeRecord(nil), outcomes...)
	sort.SliceStable(ordered, func(i, j int) bool { return outcomeCompletedAt(ordered[i]).Before(outcomeCompletedAt(ordered[j])) })
	stats := map[string]ProgramTrust{}
	for _, outcome := range ordered {
		if outcome == nil {
			continue
		}
		revisions := map[string]string{}
		for _, run := range outcome.GetProgramRuns() {
			revision := run.GetProgramRevision()
			if revision == "" {
				revision = outcome.GetSourceCommit()
			}
			revisions[run.GetProgramId()] = revision
			state := stats[run.GetProgramId()]
			if revision != "" && revision != state.LastChange {
				stats[run.GetProgramId()] = ProgramTrust{LastChange: revision}
			}
		}
		for _, application := range outcome.GetPlanApplications() {
			if !application.GetApplied() {
				continue
			}
			for _, id := range application.GetProgramIds() {
				state := stats[id]
				revision := revisions[id]
				if revision == "" {
					revision = outcome.GetSourceCommit()
				}
				if revision != state.LastChange {
					state = ProgramTrust{LastChange: revision}
				}
				failed := application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH
				for _, signal := range outcome.GetProgramFailureSignals() {
					if strings.HasSuffix(signal, ":"+id) {
						failed = true
						break
					}
				}
				if failed {
					state.Failures++
					state.VerifiedSuccesses = 0
				} else {
					state.VerifiedSuccesses++
				}
				state.Trusted = state.VerifiedSuccesses >= 3 && state.Failures == 0
				stats[id] = state
			}
		}
	}
	return stats
}
