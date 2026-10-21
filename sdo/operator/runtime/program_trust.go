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
// Accepted program revisions have passed the broker's corpus replay gate.
func ProgramTrustStats(outcomes []*contracts.OutcomeRecord, threshold ...int) map[string]ProgramTrust {
	minimum := 3
	if len(threshold) > 0 && threshold[0] > 0 {
		minimum = threshold[0]
	}
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
		failedInApplication := map[string]bool{}
		for _, application := range outcome.GetPlanApplications() {
			if !application.GetApplied() && !strings.HasPrefix(application.GetApplicationId(), "fast-path") {
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
				failed := application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != "" || application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH
				for _, signal := range outcome.GetProgramFailureSignals() {
					if strings.HasSuffix(signal, ":"+id) {
						failed = true
						break
					}
				}
				if failed {
					state.Failures++
					state.VerifiedSuccesses = 0
					failedInApplication[id] = true
				} else {
					state.VerifiedSuccesses++
				}
				state.Trusted = state.VerifiedSuccesses >= minimum && state.Failures == 0
				stats[id] = state
			}
		}
		for _, signal := range outcome.GetProgramFailureSignals() {
			id, ok := strings.CutPrefix(signal, "fast_path_failure:")
			if !ok || id == "" || failedInApplication[id] {
				continue
			}
			state := stats[id]
			revision := revisions[id]
			if revision == "" {
				revision = outcome.GetSourceCommit()
			}
			if revision != state.LastChange {
				state = ProgramTrust{LastChange: revision}
			}
			state.Failures++
			state.VerifiedSuccesses = 0
			state.Trusted = false
			stats[id] = state
			failedInApplication[id] = true
		}
	}
	return stats
}
