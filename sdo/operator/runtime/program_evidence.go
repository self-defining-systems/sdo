package runtime

import (
	"encoding/json"
	"strings"

	"sdo.dev/operator/sdk/program"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

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

// parseApplicationFailureSignal decodes "application_verification_failure:<id>[@<revision>]".
func parseApplicationFailureSignal(signal string) (id, revision string, ok bool) {
	payload, ok := strings.CutPrefix(signal, "application_verification_failure:")
	if !ok {
		return "", "", false
	}
	id, revision, bound := strings.Cut(payload, "@")
	if id == "" || strings.ContainsAny(id, ":@") || (bound && (revision == "" || strings.ContainsAny(revision, ":@"))) {
		return "", "", false
	}
	return id, revision, true
}
