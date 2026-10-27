package runtime

import (
	"context"
	"log/slog"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// ProgramCount is how often a learned program's applications ended well or
// badly in earlier incidents. It is information for the agent: it filters no
// match and authorizes no write.
type ProgramCount struct {
	Successes int `json:"successes"`
	Failures  int `json:"failures"`
}

func programCountKey(id, revision string) string { return id + "@" + revision }

// ProgramCounts tallies applied, non-dry-run applications per (program id,
// revision). An application succeeds when its incident closed healthy and it
// was neither rolled back nor errored. A recorded
// application_verification_failure counts once, unless an application of that
// program already counted as failed in the same incident.
func ProgramCounts(outcomes []*contracts.OutcomeRecord) map[string]ProgramCount {
	counts := map[string]ProgramCount{}
	for _, outcome := range outcomes {
		revisions := programRunRevisions(outcome)
		revisionOf := func(id string) string {
			if revision := revisions[id]; revision != "" {
				return revision
			}
			return outcome.GetSourceCommit()
		}
		failedHere := map[string]bool{}
		for _, application := range outcome.GetPlanApplications() {
			if !application.GetApplied() || application.GetDryRun() || application.GetRolloutPending() != nil {
				continue
			}
			failed := outcome.GetClassification() != "success" || application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != ""
			for _, id := range application.GetProgramIds() {
				key := programCountKey(id, revisionOf(id))
				count := counts[key]
				if failed {
					count.Failures++
					failedHere[id] = true
				} else {
					count.Successes++
				}
				counts[key] = count
			}
		}
		for _, signal := range outcome.GetProgramFailureSignals() {
			id, revision, ok := parseApplicationFailureSignal(signal)
			if !ok || failedHere[id] {
				continue
			}
			if revision == "" {
				revision = revisionOf(id)
			}
			key := programCountKey(id, revision)
			count := counts[key]
			count.Failures++
			counts[key] = count
			failedHere[id] = true
		}
	}
	return counts
}

// programCounts reads the counts shown to the agent. Outcomes that
// cannot be read are skipped and reported in one line; a failed listing yields
// no counts rather than blocking the incident.
func (d KubernetesJobDispatcher) programCounts(ctx context.Context) map[string]ProgramCount {
	if d.Outcomes == nil {
		return nil
	}
	stored, notes, err := memory.ListOutcomes(ctx, d.Outcomes, "program counts")
	if err != nil {
		slog.WarnContext(ctx, "program counts unavailable", "error", err)
		return nil
	}
	records := make([]*contracts.OutcomeRecord, 0, len(stored))
	for _, outcome := range stored {
		if outcome.Record != nil {
			records = append(records, outcome.Record)
		}
	}
	if skipped := len(notes) + len(stored) - len(records); skipped > 0 {
		slog.WarnContext(ctx, "program counts skipped unreadable outcomes", "skipped", skipped)
	}
	return ProgramCounts(records)
}
