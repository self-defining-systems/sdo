package runtime

import (
	"errors"
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Wire validation bounds individual fields. These cross-record invariants keep
// a corrupt/partial journal from inventing allowance or losing accepted work.
func validateSelectionLedger(s *contracts.SelectionLedger) error {
	invalid := errors.New("inconsistent durable selection ledger")
	if s == nil || len(s.SourceHistory) == 0 {
		return invalid
	}
	for _, source := range s.SourceHistory {
		if source == "" {
			return invalid
		}
	}
	hasCheckpoint := s.ProviderSessionId != ""
	if hasCheckpoint != (s.Provider != "") || hasCheckpoint != (s.CheckpointDigest != "") {
		return invalid
	}
	requestIDs, attemptIDs := map[string]bool{}, map[string]bool{}
	reserved, charged := uint64(0), uint64(0)
	for _, a := range s.Attempts {
		if a == nil {
			return invalid
		}
		if requestIDs[a.RequestId] || attemptIDs[a.AttemptId] || a.AcceptanceEpoch > s.DispatchEpoch || a.ChargedBatches > a.ReservedBatches {
			return invalid
		}
		requestIDs[a.RequestId] = true
		attemptIDs[a.AttemptId] = true
		if a.Report == nil {
			if a.ChargedBatches != 0 || a.DeliveryAcknowledged || a.ClosureStarted || a.ClosureAcknowledged {
				return invalid
			}
			reserved += uint64(a.ReservedBatches)
		} else {
			r := a.Report
			if r.IncidentId != s.IncidentId || r.SessionId != s.SessionId || r.AttemptId != a.AttemptId || r.RequestId != a.RequestId || r.AcceptanceEpoch != a.AcceptanceEpoch || r.RemainingBatches > s.TotalBatches {
				return invalid
			}
			charged += uint64(a.ChargedBatches)
		}
		if a.ClosureAcknowledged && !a.ClosureStarted {
			return invalid
		}
		actions := map[string]bool{}
		forward := map[string]bool{}
		for _, action := range a.Actions {
			if action == nil {
				return invalid
			}
			if action.RoundIndex > a.ReservedBatches || action.Receipt == nil {
				return invalid
			}
			key := action.Phase + ":" + action.Receipt.ActionId
			if actions[key] {
				return invalid
			}
			actions[key] = true
			if action.Complete != (action.Receipt.State == contracts.MitigationActionReceipt_STATE_APPLIED) {
				return invalid
			}
			if action.Phase == "forward" {
				forward[action.Receipt.ActionId] = action.Complete
			}
			if action.Phase == "compensation" && !forward[action.Receipt.ActionId] {
				return invalid
			}
		}
		captures, occurrences := map[string]bool{}, map[string]map[uint32]bool{}
		for _, capture := range a.Barriers {
			if capture == nil {
				return invalid
			}
			key := fmt.Sprintf("%d:%s", capture.RoundIndex, capture.Phase)
			if occurrences[key] == nil {
				occurrences[key] = map[uint32]bool{}
			}
			if capture.AttemptId != a.AttemptId || capture.RoundIndex > a.ReservedBatches || captures[capture.CaptureId] || occurrences[key][capture.Occurrence] {
				return invalid
			}
			captures[capture.CaptureId] = true
			occurrences[key][capture.Occurrence] = true
		}
	}
	if reserved != uint64(s.ReservedBatches) || charged > uint64(s.UsedBatches) {
		return invalid
	}
	return nil
}
