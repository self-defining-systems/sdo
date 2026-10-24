package runtime

import (
	"encoding/json"
	"fmt"
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
// failed application breaks its success streak; broker-validated revisions retain credit.
// Accepted program revisions have passed the broker's corpus replay gate.
func ProgramTrustStats(outcomes []*contracts.OutcomeRecord, threshold ...int) map[string]ProgramTrust {
	minimum := 1
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
			state.LastChange = revision
			state.Failures = failures[id][revision]
			state.Trusted = state.VerifiedSuccesses >= minimum && state.Failures == 0
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
			// Pending rollout is neither an independent success nor a failure.
			// Explicit settlement and failure signals below remain authoritative.
			if application.GetRolloutPending() != nil {
				continue
			}
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
				scoped := strings.HasPrefix(application.GetApplicationId(), "fast-path") && application.CoveredFindingsVerified != nil
				failed := !scoped && (outcome.GetClassification() != "success" || application.GetDryRun() || application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != "" || application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH)
				for _, signal := range outcome.GetProgramFailureSignals() {
					// Count durable responder failures after applications, so the
					// same failure does not also count against a successful retry.
					if strings.HasPrefix(signal, "application_verification_failure:") {
						continue
					}
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
				} else if scoped && verifiedFastPathApplication(outcome, application, id, revision) && !creditedInOutcome[id] {
					state.VerifiedSuccesses++
					creditedInOutcome[id] = true
				}
				state.Trusted = state.VerifiedSuccesses >= minimum && state.Failures == 0
				stats[id] = state
			}
		}
		// Success comes only from the controller's persisted exact settlement.
		// Historical outcomes without this versioned proof earn no new credit.
		eligibility := outcome.GetReflectionEligibility()
		if eligibility.GetVersion() == 1 {
			items := append([]*contracts.ReflectionWorkItem{}, eligibility.GetUnassigned()...)
			for _, cause := range eligibility.GetCauses() {
				items = append(items, cause.GetItems()...)
			}
			for _, item := range items {
				id, revision := item.GetProgramId(), item.GetProgramRevision()
				if item.GetDisposition() != "false_match" || id == "" || revision == "" {
					continue
				}
				state := stateForRevision(id, revision)
				if !failedInApplication[id] {
					state.Failures = recordFailure(id, revision)
				}
				state.VerifiedSuccesses = 0
				state.Trusted = false
				failedInApplication[id] = true
				stats[id] = state
			}
			for _, item := range items {
				id, revision := item.GetProgramId(), item.GetProgramRevision()
				if item.GetDisposition() != "covered" || id == "" || revision == "" || revision != revisions[id] || failedInApplication[id] || creditedInOutcome[id] {
					continue
				}
				state := stateForRevision(id, revision)
				state.VerifiedSuccesses++
				state.Trusted = state.VerifiedSuccesses >= minimum && state.Failures == 0
				stats[id] = state
				creditedInOutcome[id] = true
			}
		}
		seenFailures := map[string]bool{}
		for _, signal := range outcome.GetProgramFailureSignals() {
			id, revision, ok := parseProgramTrustFailureSignal(signal)
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

// Cause-scoped fast-path credit never depends on unrelated incident closure.
// Exact write retention and bound post-apply proof remain mandatory.
func verifiedFastPathApplication(outcome *contracts.OutcomeRecord, app *contracts.PlanApplication, id, revision string) bool {
	return strings.HasPrefix(app.GetApplicationId(), "fast-path") && app.GetCoveredFindingsVerified() && app.GetApplied() && !app.GetDryRun() && !app.GetRolledBack() && app.GetError() == "" && app.GetRollbackError() == "" && app.GetVerificationCompleted() && app.GetVerificationPassed() && app.GetFinalEquivalenceVerified() && app.GetVerification().GetResult() == contracts.Verdict_RESULT_NO_MATCH && boundPostApplyNoMatch(outcome, app, id, revision)
}

// parseProgramToolCall decodes one recorded program tool call, rejecting
// errored or self-inconsistent records.
func parseProgramToolCall(call *contracts.ProgramToolCall) (ToolRequest, ToolResponse, bool) {
	var request ToolRequest
	var response ToolResponse
	if call.GetError() != "" || json.Unmarshal([]byte(call.GetRequestJson()), &request) != nil || json.Unmarshal([]byte(call.GetResponseJson()), &response) != nil || response.Error != "" || request.Command != call.GetCommand() || response.Command != call.GetCommand() {
		return ToolRequest{}, ToolResponse{}, false
	}
	return request, response, true
}

// programRunRevisions is the revision each program ran at; a later run wins.
func programRunRevisions(outcome *contracts.OutcomeRecord) map[string]string {
	revisions := make(map[string]string)
	for _, run := range outcome.GetProgramRuns() {
		revisions[run.GetProgramId()] = run.GetProgramRevision()
	}
	return revisions
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
		request, response, ok := parseProgramToolCall(call)
		if !ok {
			continue
		}
		if call.GetCommand() == "plan" && response.PlanID != "" {
			plans[response.PlanID] = request
			continue
		}
		if call.GetCommand() != "apply" || response.ApplicationID != application.GetApplicationId() || call.GetAt() == nil || call.GetAt().CheckValid() != nil || call.GetAt().AsTime().Before(application.GetCompletedAt().AsTime()) {
			continue
		}
		if response.ProgramRunsOmitted > 0 {
			// The record lost runs, so absence of a bad run proves nothing.
			return false
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
				if run.ProgramID != id || !run.Bindings.Equal(plan.Bindings) {
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
// marker. Persist its JSON string array under "trust_failure_signals" only
// after a cause-scoped failure, and restore it on failed-marker handoff.
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
// owner rechecks live state without restoring applications or rollback state.
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
	return parseTrustFailureSignal(signal, "fast_path_failure:")
}

func parseProgramTrustFailureSignal(signal string) (id, revision string, ok bool) {
	if id, revision, ok = parseFastPathTrustFailureSignal(signal); ok {
		return id, revision, true
	}
	return parseTrustFailureSignal(signal, "application_verification_failure:")
}

func parseTrustFailureSignal(signal, prefix string) (id, revision string, ok bool) {
	payload, ok := strings.CutPrefix(signal, prefix)
	if !ok {
		return "", "", false
	}
	id, revision, bound := strings.Cut(payload, "@")
	if id == "" || strings.ContainsAny(id, ":@") || (bound && (revision == "" || strings.ContainsAny(revision, ":@"))) {
		return "", "", false
	}
	return id, revision, true
}
