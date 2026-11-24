package runtime

import (
	"context"
	"fmt"
	"slices"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
	"sdo.dev/operator/sdk"
)

func (c *Controller) closureVerificationDeadline(closure IncidentClosure) time.Time {
	deadline := closure.ResponderCompletedAt.Add(c.config.VerificationTimeout)
	if response := closure.Request.ResponseDeadline; !response.IsZero() && response.Before(deadline) {
		deadline = response
	}
	return deadline
}

// Only provisional closures can be returned to observation. A frozen
// payload may already be in the broker ledger even after controller restart.
func (c *Controller) provisionalClosureLocked() bool {
	return c.closureState == contracts.ClosureState_CLOSURE_STATE_PENDING &&
		c.pendingClosure != nil && !c.pendingClosure.Frozen &&
		c.pendingClosure.DispatchError == "" && (len(c.pendingClosure.FinalDetectorStates) > 0 || c.pendingClosure.HealthClearedAt != nil)
}

func (c *Controller) resumeClosureVerification(ctx context.Context, effect ClosureEffect) error {
	c.mu.Lock()
	if c.pendingClosure == nil || !sameIncidentClosure(*c.pendingClosure, effect.Closure) || c.pendingClosure.Frozen {
		c.mu.Unlock()
		return fmt.Errorf("closure changed before residual handoff")
	}
	err := c.reobserveClosureLocked(c.clock(), &effect.Closure.Request)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.PersistState(ctx)
}

// No broker has seen this closure. Return its report and receipts to the
// original incident. The responder has finished or been retired and keeps its
// original budget. Callers hold mu.
func (c *Controller) reobserveClosureLocked(now time.Time, request *IncidentRequest) error {
	closure := c.pendingClosure
	resumed := cloneIncidentRequest(&closure.Request)
	resumed.Findings = append([]sdk.Finding(nil), request.Findings...)
	resumed.JoinedKeys = append([]string(nil), request.JoinedKeys...)
	if _, err := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Reobserve, At: now}); err != nil {
		return err
	}
	if c.pendingClosure != nil {
		return fmt.Errorf("closure is no longer provisional")
	}
	c.clearIncidentLocked()
	c.incidentOpen = true
	c.currentIncidentRequest = resumed
	if report := deterministicStageReport(resumed.PriorStages); report.GetRecoveryWait().GetExitReason() == contracts.RecoveryWait_EXIT_REASON_CLEARED {
		report.RecoveryWait = nil
	}
	c.publishHeldLocked()
	c.currentIncidentResult = cloneIncidentResult(closure.Result)
	c.incidentDetectedAt = closure.DetectedAt
	c.incidentDispatchedAt = closure.DispatchedAt
	c.responderCompletedAt = closure.ResponderCompletedAt
	c.escalatedAt = closure.EscalatedAt
	c.responderDone = true
	if !slices.Equal(resumed.JoinedKeys, closure.Request.JoinedKeys) {
		c.escalateLocked(now)
	}
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.incidentFindingKeys = findingKeys(request.Findings)
	c.cleanedHelpers = append([]string(nil), closure.CleanedHelpers...)
	late := c.timeline
	c.timeline = cloneTimeline(closure.DetectorTimeline)
	for _, entry := range late {
		if slices.Contains(resumed.JoinedKeys, entry.key()) && !slices.Contains(closure.Request.JoinedKeys, entry.key()) {
			c.timeline = append(c.timeline, entry)
		}
	}
	c.lateJoinedKeys = lateJoinedFromTimeline(c.timeline)
	c.recoverySafetyLost = true
	c.cachedVerification = nil
	c.closureRecheckIdentity, c.closureRecheckSince, c.closureRecheckRetryAt = "", time.Time{}, time.Time{}
	return nil
}

// Broker command identity and current health are different durable facts. An
// already frozen command may finalize failed at its bound without rewriting
// the command that an interrupted broker attempt recorded.
type closureHealthObservation struct {
	At          time.Time
	States      []DetectorEvaluation
	Findings    []sdk.Finding
	Reason      string
	Unconfirmed contracts.ClosureUnconfirmedReason
}
type unverifiedClosureBroker interface {
	processUnverifiedClosure(context.Context, IncidentClosure, closureHealthObservation) (ClosureReceipt, error)
}

func closureObservation(at time.Time, recheck closureRecheck) *closureHealthObservation {
	observed := &closureHealthObservation{At: at, States: cloneEvaluations(recheck.States), Reason: recheck.Reason, Unconfirmed: recheck.Unconfirmed}
	for _, observation := range recheck.Observations {
		for _, finding := range observation.Findings {
			if finding.Status == sdk.FindingActive {
				observed.Findings = append(observed.Findings, finding)
			}
		}
	}
	return observed
}
func (observation closureHealthObservation) withoutRecovery(command IncidentClosure) IncidentClosure {
	result := *cloneIncidentClosure(&command)
	result.VerifiedAt = observation.At
	result.PartialDetectorStates = append(result.PartialDetectorStates, result.FinalDetectorStates...)
	result.PartialDetectorStates = append(result.PartialDetectorStates, cloneEvaluations(observation.States)...)
	result.FinalDetectorStates = nil
	result.HealthClearedAt = nil
	if result.Result != nil {
		result.Result.UnresolvedFindings = append([]sdk.Finding(nil), observation.Findings...)
	}
	if len(observation.Findings) == 0 {
		result.UnconfirmedReason = observation.Unconfirmed
		if result.UnconfirmedReason == contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_UNSPECIFIED {
			result.UnconfirmedReason = contracts.ClosureUnconfirmedReason_CLOSURE_UNCONFIRMED_REASON_OBSERVATION_INCOMPLETE
		}
		result.UnconfirmedDetail = observation.Reason
		if result.UnconfirmedDetail == "" {
			result.UnconfirmedDetail = "recovery stability window incomplete at the verification bound"
		}
	}
	return result
}
