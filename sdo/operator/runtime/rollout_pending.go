package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// DefaultMitigationRolloutTimeout leaves time to repair a peer that blocks a rollout.
const DefaultMitigationRolloutTimeout = 90 * time.Second

// RolloutPendingError means writes landed but rollout completion is unverified.
// It is neither permission to undo those writes nor independently verified repair.
type RolloutPendingError struct{ Pending *contracts.RolloutPending }

func (e *RolloutPendingError) Error() string { return "rollout_pending: " + e.Pending.GetReason() }

func (e PlanExecutor) waitForRollouts(ctx context.Context, incident IncidentRequest, refs []sdk.ObjectRef) (*contracts.RolloutPending, error) {
	budget := e.RolloutTimeout
	if budget <= 0 || budget > DefaultMitigationRolloutTimeout {
		budget = DefaultMitigationRolloutTimeout
	}
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if !incident.ResponseDeadline.IsZero() {
		var cancelDeadline context.CancelFunc
		waitCtx, cancelDeadline = context.WithDeadline(waitCtx, incident.ResponseDeadline)
		defer cancelDeadline()
	}
	err := e.Rollouts.Wait(waitCtx, refs)
	if err == nil {
		return nil, nil
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	pending := &contracts.RolloutPending{Reason: "rollout budget expired; writes remain applied and are not independently verified"}
	var blocked *rolloutBlockedError
	if errors.As(err, &blocked) {
		pending.Blockers = blocked.blockers
	}
	if len(pending.Blockers) == 0 {
		for _, ref := range refs {
			if ref.Kind == "Deployment" {
				pending.Blockers = append(pending.Blockers, &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: "rollout did not complete within the remaining budget"})
			}
		}
	}
	if len(pending.Blockers) > 0 {
		pending.Reason = fmt.Sprintf("%s/%s rollout is pending: %s", pending.Blockers[0].GetRef().GetKind(), pending.Blockers[0].GetRef().GetName(), pending.Blockers[0].GetReason())
	}
	return pending, &RolloutPendingError{Pending: pending}
}

type rolloutBlockedError struct {
	blockers []*contracts.RolloutBlocker
	cause    error
}

func (e *rolloutBlockedError) Error() string { return e.cause.Error() }
func (e *rolloutBlockedError) Unwrap() error { return e.cause }

// Truncated tool records retain a bounded reason, never a verification claim.
func compactRolloutPending(value *contracts.RolloutPending) *contracts.RolloutPending {
	if value == nil {
		return nil
	}
	result := proto.Clone(value).(*contracts.RolloutPending)
	result.Reason = boundedRolloutText(result.Reason, 96)
	if len(result.Blockers) > 1 {
		result.Blockers = result.Blockers[:1]
	}
	for _, blocker := range result.Blockers {
		blocker.Reason = boundedRolloutText(blocker.Reason, 96)
		if len(blocker.WarningEvents) > 1 {
			blocker.WarningEvents = blocker.WarningEvents[:1]
		}
		for i := range blocker.WarningEvents {
			blocker.WarningEvents[i] = boundedRolloutText(blocker.WarningEvents[i], 96)
		}
	}
	return result
}
