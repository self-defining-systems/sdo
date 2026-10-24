package runtime

import (
	"encoding/json"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// VerificationSummary is controller-owned negative evidence. It retains every
// open resource (including related resources) and detector evaluation, without
// bulky finding prose. Complete=false must never establish resource clearance.
type VerificationSummary struct {
	Version       int                      `json:"version"`
	Complete      bool                     `json:"complete"`
	FindingsCount int                      `json:"findings_count"`
	OpenResources []sdk.ObjectRef          `json:"open_resources,omitempty"`
	Evaluations   []VerificationEvaluation `json:"evaluations,omitempty"`
}

type VerificationEvaluation struct {
	DetectorID string                   `json:"detector_id"`
	At         time.Time                `json:"at"`
	Status     DetectorEvaluationStatus `json:"status"`
}

func compactVerification(response ToolResponse) *VerificationSummary {
	summary := &VerificationSummary{Version: 1, Complete: response.Command == "verify" && response.Error == "", FindingsCount: len(response.Findings)}
	seen := map[sdk.ObjectRef]bool{}
	for _, finding := range response.Findings {
		refs := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
		for _, ref := range refs {
			ref.APIVersion = ""
			if ref.Kind == "" || ref.Name == "" {
				summary.Complete = false
				continue
			}
			if !seen[ref] {
				seen[ref] = true
				summary.OpenResources = append(summary.OpenResources, ref)
			}
		}
	}
	detectors := map[string]bool{}
	for _, state := range response.DetectorStates {
		if state.DetectorID == "" || state.EvaluatedAt.IsZero() || state.Error != "" || (state.Status != DetectorEvaluationClear && state.Status != DetectorEvaluationFiring) || detectors[state.DetectorID] {
			summary.Complete = false
		}
		detectors[state.DetectorID] = true
		summary.Evaluations = append(summary.Evaluations, VerificationEvaluation{state.DetectorID, state.EvaluatedAt, state.Status})
	}
	if len(summary.Evaluations) == 0 {
		summary.Complete = false
	}
	return summary
}

// latestPartialVerification never falls back past a newer failed, omitted or
// malformed verify. It uses only controller-recorded responses, never files
// supplied by the worker or a benchmark result.
func latestPartialVerification(outcome *contracts.OutcomeRecord) (*VerificationSummary, *contracts.ProgramToolCall) {
	var latest *contracts.ProgramToolCall
	for _, call := range outcome.GetProgramToolCalls() {
		if call.GetCommand() != "verify" {
			continue
		}
		if call.GetAt() == nil || call.GetAt().CheckValid() != nil {
			return nil, nil
		}
		if latest == nil || !call.GetAt().AsTime().Before(latest.GetAt().AsTime()) {
			latest = call
		}
	}
	if latest == nil {
		return nil, nil
	}
	_, response, ok := parseProgramToolCall(latest)
	if !ok {
		return nil, nil
	}
	var record struct {
		Truncated           bool                 `json:"truncated"`
		FindingsCount       int                  `json:"findings_count"`
		DetectorStatesCount int                  `json:"detector_states_count"`
		Verification        *VerificationSummary `json:"verification_summary"`
	}
	if json.Unmarshal([]byte(latest.GetResponseJson()), &record) != nil {
		return nil, nil
	}
	if record.Truncated && record.Verification != nil && (record.FindingsCount != record.Verification.FindingsCount || record.DetectorStatesCount != len(record.Verification.Evaluations)) {
		return nil, nil
	}
	summary := record.Verification
	if !record.Truncated {
		summary = compactVerification(response)
	}
	if summary == nil || summary.Version != 1 || !summary.Complete || summary.FindingsCount < 0 || len(summary.Evaluations) == 0 {
		return nil, nil
	}
	if summary.FindingsCount == 0 {
		if !response.Healthy || response.Unresolved || len(summary.OpenResources) != 0 {
			return nil, nil
		}
	} else if response.Healthy || len(summary.OpenResources) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, state := range summary.Evaluations {
		if (response.Healthy && state.Status != DetectorEvaluationClear) || state.DetectorID == "" || seen[state.DetectorID] || state.At.IsZero() || state.At.After(latest.GetAt().AsTime()) || (state.Status != DetectorEvaluationClear && state.Status != DetectorEvaluationFiring) {
			return nil, nil
		}
		seen[state.DetectorID] = true
	}
	return summary, latest
}

func coldPartialAction(outcome *contracts.OutcomeRecord, action *contracts.RepairActionReceipt) bool {
	if !action.GetSuccess() || !validActionTimes(action) || !recordedMutation(action) {
		return false
	}
	summary, call := latestPartialVerification(outcome)
	if summary == nil || outcome.GetTimestamps().GetDispatchedAt() == nil || outcome.GetTimestamps().GetDispatchedAt().CheckValid() != nil || action.GetStartedAt().AsTime().Before(outcome.GetTimestamps().GetDispatchedAt().AsTime()) || action.GetCompletedAt().AsTime().After(call.GetAt().AsTime()) {
		return false
	}
	if outcome.GetTimestamps().GetDetectedAt() == nil || outcome.GetTimestamps().GetDetectedAt().CheckValid() != nil || action.GetStartedAt().AsTime().Before(outcome.GetTimestamps().GetDetectedAt().AsTime()) {
		return false
	}
	for _, resource := range action.GetResources() {
		ref := reflectionResource(resource)
		if ref.Kind == "" || ref.Name == "" {
			return false
		}
		for _, open := range summary.OpenResources {
			open.APIVersion = ""
			if open == ref {
				return false
			}
		}
		for _, peer := range outcome.GetRepairActions() {
			if !peer.GetSuccess() {
				continue
			}
			for _, touched := range peer.GetResources() {
				if reflectionResource(touched) == ref && (!validActionTimes(peer) || peer.GetCompletedAt().AsTime().After(call.GetAt().AsTime())) {
					return false
				}
			}
		}
		triggered := false
		for _, finding := range outcome.GetFindings() {
			// Related-only trigger attribution is ambiguous: require a primary trigger.
			primary := reflectionResource(finding.GetPrimaryResource()) == ref
			related := false
			for _, resource := range finding.GetRelatedResources() {
				related = related || reflectionResource(resource) == ref
			}
			if !primary && !related {
				continue
			}
			triggered = triggered || primary
			evaluated := false
			for _, state := range summary.Evaluations {
				if state.DetectorID == finding.GetDetectorId() && !state.At.Before(action.GetCompletedAt().AsTime()) {
					evaluated = true
				}
			}
			if !evaluated {
				return false
			}
		}
		if !triggered {
			return false
		}
	}
	return len(action.GetResources()) > 0
}

func validActionTimes(action *contracts.RepairActionReceipt) bool {
	return action.GetStartedAt() != nil && action.GetStartedAt().CheckValid() == nil && action.GetCompletedAt() != nil && action.GetCompletedAt().CheckValid() == nil && !action.GetCompletedAt().AsTime().Before(action.GetStartedAt().AsTime())
}

func recordedMutation(action *contracts.RepairActionReceipt) bool {
	if !strings.HasPrefix(action.GetActionId(), "sdo-act-") || action.GetActionId() == "sdo-act-" {
		return false
	}
	switch action.GetKind() {
	case "kubectl-patch", "kubectl-apply", "kubectl-delete", "kubectl-scale":
		return true
	}
	return false
}
