package runtime

import (
	"fmt"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// MenuRepairActions projects actual safe effects into ordinary incident repair
// evidence, without creating legacy PlanApplications or granting program trust.
func (l *ResponderLoop) MenuRepairActions() []RepairActionReceipt {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.MenuConfig != nil && l.MenuConfig.Direct {
		return append([]RepairActionReceipt(nil), l.menuRepairs...)
	}
	if l.selection == nil {
		return nil
	}
	var receipts []RepairActionReceipt
	for _, attempt := range l.selection.snapshot().Attempts {
		if attempt.Report == nil {
			continue
		}
		starts := map[uint32]time.Time{}
		ends := map[uint32]time.Time{}
		for _, barrier := range attempt.Barriers {
			if !barrier.Complete {
				continue
			}
			at := barrier.ObservedAt.AsTime()
			if starts[barrier.RoundIndex].IsZero() {
				starts[barrier.RoundIndex] = at
			}
			ends[barrier.RoundIndex] = at
		}
		for _, round := range attempt.Report.Rounds {
			for _, action := range round.Actions {
				if action.State != contracts.MitigationActionReceipt_STATE_APPLIED {
					continue
				}
				success := action.CompensationState == contracts.MitigationActionReceipt_STATE_UNSPECIFIED
				receipts = append(receipts, RepairActionReceipt{ActionID: action.ActionId, Kind: "mitigation-menu", Target: action.Resource.Kind + "/" + action.Resource.Name, Resources: []sdk.ObjectRef{objectRefState(action.Resource)}, Summary: "controller applied SDK mitigation action", Details: fmt.Sprintf("attempt %s round %d; compensation %s", attempt.AttemptId, round.RoundIndex, action.CompensationState), StartedAt: starts[round.RoundIndex], CompletedAt: ends[round.RoundIndex], Success: success, Reversible: false})
			}
		}
	}
	return receipts
}
