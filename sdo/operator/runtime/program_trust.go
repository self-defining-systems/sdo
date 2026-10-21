package runtime

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"

	"sdo.dev/operator/sdk/program"

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
	// Keep failures by dependency revision, including across a later reversion.
	failures := map[string]map[string]int{}
	stateForRevision := func(id, revision string) ProgramTrust {
		state := stats[id]
		if revision != state.LastChange {
			state = ProgramTrust{LastChange: revision, Failures: failures[id][revision]}
		}
		return state
	}
	recordFailure := func(id, revision string) int {
		if failures[id] == nil {
			failures[id] = map[string]int{}
		}
		failures[id][revision]++
		return failures[id][revision]
	}
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
				stats[run.GetProgramId()] = stateForRevision(run.GetProgramId(), revision)
			}
		}
		failedInApplication := map[string]bool{}
		creditedInOutcome := map[string]bool{}
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
				state = stateForRevision(id, revision)
				failed := outcome.GetClassification() != "success" || application.GetDryRun() || application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != "" || application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH
				for _, signal := range outcome.GetProgramFailureSignals() {
					failedID, failedRevision, fastFailure := parseFastPathTrustFailureSignal(signal)
					if strings.HasSuffix(signal, ":"+id) || (fastFailure && failedID == id && (failedRevision == "" || failedRevision == revision)) {
						failed = true
						break
					}
				}
				if failed {
					state.Failures = recordFailure(id, revision)
					state.VerifiedSuccesses = 0
					failedInApplication[id] = true
				} else if !boundPostApplyNoMatch(outcome, application, id, revision) {
					// Incomplete or inconclusive evidence cannot extend a streak.
					state.VerifiedSuccesses = 0
					failedInApplication[id] = true
				} else if !creditedInOutcome[id] && !failedInApplication[id] {
					creditedInOutcome[id] = true
					state.VerifiedSuccesses++
				}
				state.Trusted = state.VerifiedSuccesses >= minimum && state.Failures == 0
				stats[id] = state
			}
		}
		seenFailures := map[string]bool{}
		for _, signal := range outcome.GetProgramFailureSignals() {
			id, revision, ok := parseFastPathTrustFailureSignal(signal)
			if !ok {
				continue
			}
			if revision == "" {
				revision = revisions[id]
			}
			if revision == "" {
				revision = stats[id].LastChange
			}
			if revision == "" {
				revision = outcome.GetSourceCommit()
			}
			key := id + "@" + revision
			if seenFailures[key] {
				continue
			}
			seenFailures[key] = true
			// An application may already have counted this exact failure.
			if failedInApplication[id] && stats[id].LastChange == revision && failures[id][revision] > 0 {
				continue
			}
			count := recordFailure(id, revision)
			state := stats[id]
			if state.LastChange != "" && state.LastChange != revision {
				continue
			}
			state.LastChange = revision
			state.Failures = count
			state.VerifiedSuccesses = 0
			state.Trusted = false
			stats[id] = state
		}
	}
	return stats
}

// boundPostApplyNoMatch requires controller-recorded tool evidence. The plan
// request supplies the applied binding; the matching apply response supplies
// its post-write diagnosis. Flat ProgramRuns and application.Verification alone
// have no binding-scoped post-apply ordering (the latter is health-only).
// Missing or truncated history deliberately earns no credit.
func boundPostApplyNoMatch(outcome *contracts.OutcomeRecord, application *contracts.PlanApplication, id, revision string) bool {
	if revision == "" || application.GetApplicationId() == "" || application.GetCompletedAt() == nil || application.GetCompletedAt().CheckValid() != nil {
		return false
	}
	for _, run := range outcome.GetProgramRuns() {
		if run.GetProgramId() != id {
			continue
		}
		observedRevision := run.GetProgramRevision()
		if observedRevision == "" {
			observedRevision = outcome.GetSourceCommit()
		}
		if observedRevision != revision {
			return false
		}
	}
	plans := map[string]ToolRequest{}
	for _, call := range outcome.GetProgramToolCalls() {
		var request ToolRequest
		var response ToolResponse
		if call.GetError() != "" || json.Unmarshal([]byte(call.GetRequestJson()), &request) != nil || json.Unmarshal([]byte(call.GetResponseJson()), &response) != nil || response.Error != "" || request.Command != call.GetCommand() || response.Command != call.GetCommand() {
			continue
		}
		if call.GetCommand() == "plan" && response.PlanID != "" {
			plans[response.PlanID] = request
			continue
		}
		if call.GetCommand() != "apply" || response.ApplicationID != application.GetApplicationId() || call.GetAt() == nil || call.GetAt().CheckValid() != nil || call.GetAt().AsTime().Before(application.GetCompletedAt().AsTime()) {
			continue
		}
		selected := false
		for _, planID := range request.PlanIDs {
			plan, exists := plans[planID]
			if !exists {
				return false
			}
			if plan.ProgramID != id {
				continue
			}
			selected = true
			verified := false
			for _, run := range response.ProgramRuns {
				if run.ProgramID != id || !maps.Equal(run.Bindings, plan.Bindings) {
					continue
				}
				if run.ProgramRevision != "" && run.ProgramRevision != revision {
					return false
				}
				if run.Verdict.Result != program.NoMatch || programRunContract(run).GetError() != "" {
					return false
				}
				verified = true
			}
			if !verified {
				return false
			}
		}
		return selected
	}
	return false
}

// fastPathTrustFailureSignals is the durable trust input for a fast-path
// marker. Persist its JSON string array under "trust_failure_signals" before
// application, retain it through failed recovery, and restore it on handoff.
// The revision comes from the accepted trust snapshot, not the source commit
// of a later responder outcome.
func (l *ResponderLoop) fastPathTrustFailureSignals(ids []string) []string {
	signals := make([]string, 0, len(ids))
	for _, id := range ids {
		revision := l.Trust[id].LastChange
		if revision == "" {
			revision = l.Request.SourceCommit
		}
		signal := "fast_path_failure:" + id
		if revision != "" {
			signal += "@" + revision
		}
		signals = append(signals, signal)
	}
	return signals
}

// restoreFastPathTrustFailureSignals restores only trust evidence. The marker
// owner remains responsible for restoring applications and rollback state.
func (l *ResponderLoop) restoreFastPathTrustFailureSignals(encoded string) error {
	if encoded == "" {
		return nil
	}
	var signals []string
	if err := json.Unmarshal([]byte(encoded), &signals); err != nil {
		return fmt.Errorf("decode fast-path trust failures: %w", err)
	}
	for _, signal := range signals {
		if _, _, ok := parseFastPathTrustFailureSignal(signal); !ok {
			return fmt.Errorf("invalid fast-path trust failure signal %q", signal)
		}
	}
	for _, signal := range signals {
		id, _, _ := parseFastPathTrustFailureSignal(signal)
		l.addFailureSignal(signal)
		exists := false
		for _, prior := range l.fastPathPrograms {
			exists = exists || prior == id
		}
		if !exists {
			l.fastPathPrograms = append(l.fastPathPrograms, id)
		}
	}
	return nil
}

func parseFastPathTrustFailureSignal(signal string) (id, revision string, ok bool) {
	payload, ok := strings.CutPrefix(signal, "fast_path_failure:")
	if !ok {
		return "", "", false
	}
	id, revision, bound := strings.Cut(payload, "@")
	if id == "" || strings.ContainsAny(id, ":@") || (bound && (revision == "" || strings.ContainsAny(revision, ":@"))) {
		return "", "", false
	}
	return id, revision, true
}
