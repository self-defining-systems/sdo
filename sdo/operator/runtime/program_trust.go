package runtime

import (
	"encoding/json"
	"sort"

	"sdo.dev/operator/sdk/program"
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
				if revision != state.LastChange {
					state = ProgramTrust{LastChange: revision}
				}
				failed := outcome.GetClassification() != "success" || application.GetDryRun() || application.GetError() != "" || application.GetRolledBack() || application.GetRollbackError() != "" || application.GetVerification().GetResult() != contracts.Verdict_RESULT_NO_MATCH
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
				if run.ProgramID != id || bindingKey(run.Bindings) != bindingKey(plan.Bindings) {
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
