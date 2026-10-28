package runtime

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"time"
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
	observer, canObserve := e.Rollouts.(interface {
		Observe(context.Context, []sdk.ObjectRef) error
	})
	if e.PollRollouts && !canObserve {
		budget = time.Millisecond
	}
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if !incident.ResponseDeadline.IsZero() {
		var cancelDeadline context.CancelFunc
		waitCtx, cancelDeadline = context.WithDeadline(waitCtx, incident.ResponseDeadline)
		defer cancelDeadline()
	}
	var err error
	if e.PollRollouts && canObserve {
		err = observer.Observe(waitCtx, refs)
	} else {
		err = e.Rollouts.Wait(waitCtx, refs)
	}
	if err == nil {
		return nil, nil
	}
	var admission *AdmissionBlockedError
	if errors.As(err, &admission) {
		return admission.Pending, admission
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errRolloutStalled) {
		return nil, err
	}
	// A stalled rollout is handed off as pending: its writes stay applied, and
	// the blocker names what another actor must change.
	pending := &contracts.RolloutPending{Reason: "rollout is settling; writes remain applied and are not independently verified"}
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
	if len(result.AdmissionConfigurations) > 1 {
		result.AdmissionConfigurations = result.AdmissionConfigurations[:1]
	}
	if len(result.AdmissionWebhooks) > 1 {
		result.AdmissionWebhooks = result.AdmissionWebhooks[:1]
	}
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
