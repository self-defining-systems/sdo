package runtime

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// recover settles accepted work with read-only observations. Neither plans nor
// rollback pre-images survive, so mutations and compensation are never replayed.
// verify must use the direct-reader barrier; nil is an explicit evidence gap.
func (j *selectionJournal) recover(ctx context.Context, verify func(context.Context, *contracts.SelectionAttempt) (*contracts.MitigationBarrier, error)) error {
	for _, a := range j.snapshot().Attempts {
		if a.Report != nil {
			continue
		}
		s := j.snapshot()
		report := &contracts.MitigationReport{Version: 1, IncidentId: s.IncidentId, SessionId: s.SessionId, RequestId: a.RequestId, AttemptId: a.AttemptId, AcceptanceEpoch: a.AcceptanceEpoch, Status: contracts.MitigationReport_STATUS_INTERRUPTED, FailedPhase: "recovery", ReasonCode: "selection_interrupted", WritesFrozen: s.WritesFrozen, Rounds: a.Rounds, Barriers: a.Barriers}
		used := uint32(0)
		seen := map[uint32]*contracts.MitigationRoundReport{}
		for _, r := range report.Rounds {
			seen[r.RoundIndex] = r
		}
		for _, action := range a.Actions {
			if action.RoundIndex > used {
				used = action.RoundIndex
			}
			receipt := proto.Clone(action.Receipt).(*contracts.MitigationActionReceipt)
			if !action.Complete {
				receipt.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
			}
			report.Residuals = append(report.Residuals, proto.Clone(receipt.Resource).(*contracts.ObjectRef))
			round := seen[action.RoundIndex]
			if round == nil {
				round = &contracts.MitigationRoundReport{RoundIndex: action.RoundIndex, RoundId: fmt.Sprintf("%s-round-%d", a.AttemptId, action.RoundIndex)}
				seen[action.RoundIndex] = round
				report.Rounds = append(report.Rounds, round)
			}
			// Completed rounds already retain their safe receipts. An interrupted
			// action missing from those rounds must still be visible to recovery.
			found := false
			for _, existing := range round.Actions {
				if existing.ActionId == receipt.ActionId {
					if action.Phase == "compensation" {
						existing.CompensationState = receipt.State
					} else if !action.Complete {
						existing.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
					}
					found = true
					break
				}
			}
			if !found {
				if action.Phase == "compensation" {
					receipt.CompensationState = receipt.State
					receipt.State = contracts.MitigationActionReceipt_STATE_UNKNOWN
				}
				round.Actions = append(round.Actions, receipt)
			}
		}
		var barrier *contracts.MitigationBarrier
		var err error
		if verify != nil {
			barrier, err = verify(ctx, proto.Clone(a).(*contracts.SelectionAttempt))
		} else {
			err = errors.New("recovery direct reader unavailable")
		}
		if err != nil || barrier == nil || !barrier.Complete || barrier.Phase != "recovery" || barrier.AttemptId != a.AttemptId {
			report.ReasonCode = "recovery_observation_unavailable"
		} else {
			report.Barriers = append(report.Barriers, barrier)
		}
		if !j.now().Before(s.OriginalDeadline.AsTime()) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			report.Status = contracts.MitigationReport_STATUS_TIMEOUT
			report.ReasonCode = "original_deadline_expired"
		}
		if _, err := j.settle(ctx, a.AttemptId, report, used); err != nil {
			return err
		}
	}
	return nil
}
