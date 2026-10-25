package runtime

import (
	"slices"
	"sort"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// reflectionDetectorStates keeps the global closure verdict separate from
// per-finding repair evidence. A failed closure retains latest post-response
// observations, including unresolved peers and incomplete confirmation windows.
// Called with c.mu held; superseded observation scopes supply no repair proof.
func (c *Controller) reflectionDetectorStates(verified, scopeCurrent bool) []DetectorEvaluation {
	if !scopeCurrent {
		return nil
	}
	if verified {
		return c.incidentDetectorStates()
	}
	ids := append([]string(nil), c.healthDetectorIDs...)
	for _, finding := range c.currentIncidentRequest.Findings {
		if !slices.Contains(ids, finding.DetectorID) {
			ids = append(ids, finding.DetectorID)
		}
	}
	sort.Strings(ids)
	states, _ := c.latestEvaluations(ids, c.responderCompletedAt, false)
	for i := range states {
		state := &states[i]
		if state.Status != DetectorEvaluationClear && state.Status != DetectorEvaluationFiring {
			continue
		}
		state.Fingerprints = append([]string(nil), state.Fingerprints...)
		// Raw absence before Clearing persistence completes is not confirmed clear.
		for _, finding := range c.tracker.ActiveFindings([]string{state.DetectorID}, nil) {
			fingerprint := FindingFingerprint(finding)
			if !slices.Contains(state.Fingerprints, fingerprint) {
				state.Fingerprints = append(state.Fingerprints, fingerprint)
			}
		}
		if len(state.Fingerprints) > 0 {
			state.Status = DetectorEvaluationFiring
		}
		sort.Strings(state.Fingerprints)
	}
	return states
}

// closurePartialAction binds trusted mutation receipts to opening findings and
// the last independent post-response detector verdict, without requiring an
// agent-issued verify command. Aggregate failure never supplies repair proof.
func closurePartialAction(outcome *contracts.OutcomeRecord, action *contracts.RepairActionReceipt) bool {
	if !action.GetSuccess() || action.GetFailureReason() != "" || !validActionTimes(action) || !recordedMutation(action) || len(action.GetResources()) == 0 {
		return false
	}
	times := outcome.GetTimestamps()
	if times.GetDetectedAt() == nil || !times.GetDetectedAt().IsValid() || times.GetDispatchedAt() == nil || !times.GetDispatchedAt().IsValid() || times.GetCompletedAt() == nil || !times.GetCompletedAt().IsValid() || action.GetStartedAt().AsTime().Before(times.GetDetectedAt().AsTime()) || action.GetStartedAt().AsTime().Before(times.GetDispatchedAt().AsTime()) {
		return false
	}
	for _, resource := range action.GetResources() {
		ref := reflectionResource(resource)
		if ref.Kind == "" || ref.Name == "" {
			return false
		}
		lastMutation := action.GetCompletedAt().AsTime()
		for _, peer := range outcome.GetRepairActions() {
			if !peer.GetSuccess() {
				continue
			}
			for _, touched := range peer.GetResources() {
				if reflectionResource(touched) == ref {
					if !validActionTimes(peer) {
						return false
					}
					if peer.GetCompletedAt().AsTime().After(lastMutation) {
						lastMutation = peer.GetCompletedAt().AsTime()
					}
				}
			}
		}
		triggered := false
		for _, finding := range outcome.GetFindings() {
			primary := reflectionResource(finding.GetPrimaryResource()) == ref
			related := slices.ContainsFunc(finding.GetRelatedResources(), func(r *contracts.ObjectRef) bool { return reflectionResource(r) == ref })
			if !primary && !related {
				continue
			}
			if finding.GetStatus() != "active" || finding.GetDetectorId() == "" || finding.GetFingerprint() == "" {
				return false
			}
			triggered = triggered || primary
			var latest *contracts.DetectorEvaluation
			for _, evaluation := range outcome.GetDetectorHistory() {
				if evaluation.GetDetectorId() == finding.GetDetectorId() && evaluation.GetEvaluatedAt() != nil && evaluation.GetEvaluatedAt().IsValid() && (latest == nil || !evaluation.GetEvaluatedAt().AsTime().Before(latest.GetEvaluatedAt().AsTime())) {
					latest = evaluation
				}
			}
			if latest == nil || latest.GetError() != "" || latest.GetEvaluatedAt().AsTime().Before(times.GetCompletedAt().AsTime()) || latest.GetEvaluatedAt().AsTime().Before(lastMutation) {
				return false
			}
			clear := latest.GetStatus() == "clear" && len(latest.GetFingerprints()) == 0
			peerOnly := latest.GetStatus() == "firing" && len(latest.GetFingerprints()) > 0 && !slices.Contains(latest.GetFingerprints(), finding.GetFingerprint())
			if !clear && !peerOnly {
				return false
			}
		}
		if !triggered {
			return false
		}
	}
	return true
}

// Earlier tool/plan proof cannot override a newer controller observation that
// the same finding returned or that its confirmation became uncertain.
func partialActionVerified(outcome *contracts.OutcomeRecord, action *contracts.RepairActionReceipt, proof map[sdk.ObjectRef]time.Time, applications map[*contracts.PlanApplication]bool) bool {
	if !verifiedPartialAction(action, proof, applications) && !coldPartialAction(outcome, action) && !closurePartialAction(outcome, action) {
		return false
	}
	completed := outcome.GetTimestamps().GetCompletedAt()
	if completed == nil || !completed.IsValid() {
		return true
	}
	for _, finding := range outcome.GetFindings() {
		refs := append([]*contracts.ObjectRef{finding.GetPrimaryResource()}, finding.GetRelatedResources()...)
		touched := slices.ContainsFunc(refs, func(ref *contracts.ObjectRef) bool {
			return slices.ContainsFunc(action.GetResources(), func(r *contracts.ObjectRef) bool { return reflectionResource(r) == reflectionResource(ref) })
		})
		if !touched {
			continue
		}
		var latest *contracts.DetectorEvaluation
		for _, evaluation := range outcome.GetDetectorHistory() {
			if evaluation.GetDetectorId() == finding.GetDetectorId() && evaluation.GetEvaluatedAt() != nil && evaluation.GetEvaluatedAt().IsValid() && !evaluation.GetEvaluatedAt().AsTime().Before(completed.AsTime()) && (latest == nil || !evaluation.GetEvaluatedAt().AsTime().Before(latest.GetEvaluatedAt().AsTime())) {
				latest = evaluation
			}
		}
		if latest == nil {
			continue
		}
		clear := latest.GetError() == "" && latest.GetStatus() == "clear" && len(latest.GetFingerprints()) == 0
		peerOnly := latest.GetError() == "" && latest.GetStatus() == "firing" && len(latest.GetFingerprints()) > 0 && finding.GetFingerprint() != "" && !slices.Contains(latest.GetFingerprints(), finding.GetFingerprint())
		if !clear && !peerOnly {
			return false
		}
	}
	return true
}
