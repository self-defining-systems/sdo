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
// revision), once per application. An application of a failed incident, or one
// that errored or was rolled back, is a failure; an application in a successful
// incident is a success. A cleared_without_action or unconfirmed closure is
// neither: the first needed no repair and the second is verified but not a
// failure, so its applications carry no verdict.
//
// An application whose own record shows a completed verification that did not
// pass is a failure too. A recorded application_verification_failure signal
// names a program, not an application: when no application of that program
// failed in the incident, the signal stands for one whose record was lost
// (a restart discards application history) and adds one failure against the
// signal's revision. An application whose record shows the failure is
// therefore counted once, never as both.
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
		failedKeys := map[string]bool{}
		tally := func(key string, verdict applicationOutcome) {
			count := counts[key]
			if verdict == failedApplication {
				count.Failures++
			} else {
				count.Successes++
			}
			counts[key] = count
		}
		for _, application := range outcome.GetPlanApplications() {
			if !application.GetApplied() || application.GetDryRun() || application.GetRolloutPending() != nil {
				continue
			}
			harmed := application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != "" ||
				(application.GetVerificationCompleted() && !application.GetVerificationPassed())
			verdict := applicationVerdict(outcome.GetClassification(), harmed)
			if verdict == noVerdict {
				continue
			}
			for _, id := range application.GetProgramIds() {
				key := programCountKey(id, revisionOf(id))
				tally(key, verdict)
				if verdict == failedApplication {
					failedKeys[id] = true
				}
			}
		}
		for _, signal := range outcome.GetProgramFailureSignals() {
			id, revision, ok := parseApplicationFailureSignal(signal)
			if !ok || failedKeys[id] {
				continue
			}
			failedKeys[id] = true
			if revision == "" {
				revision = revisionOf(id)
			}
			tally(programCountKey(id, revision), failedApplication)
		}
	}
	return counts
}

type applicationOutcome int

const (
	noVerdict applicationOutcome = iota
	succeededApplication
	failedApplication
)

// applicationVerdict maps one application's incident classification and own
// harm to a count. Classifications that carry no verdict are listed, so a new
// closure kind is counted as a failure only if it is not one of them.
func applicationVerdict(classification string, harmed bool) applicationOutcome {
	switch classification {
	case "cleared_without_action", "unconfirmed":
		return noVerdict
	case "success":
		if harmed {
			return failedApplication
		}
		return succeededApplication
	default:
		return failedApplication
	}
}

// countsFromCoverage is the count view the agent was shown: the matches of the
// frozen coverage, keyed like ProgramCounts.
func countsFromCoverage(coverage *contracts.PlaybookCoverage) map[string]ProgramCount {
	counts := map[string]ProgramCount{}
	for _, match := range coverage.GetMatches() {
		counts[programCountKey(match.GetPlaybookId(), match.GetProgramRevision())] = ProgramCount{
			Successes: int(match.GetSuccessfulApplications()), Failures: int(match.GetFailedApplications()),
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
