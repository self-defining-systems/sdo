package runtime

import (
	"context"
	"fmt"
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

// No broker has seen an unfrozen closure. Return its original work and receipts
// to the existing residual/continuation path, without a fresh incident budget.
func (c *Controller) resumeClosureVerification(ctx context.Context, effect ClosureEffect) error {
	c.mu.Lock()
	if c.pendingClosure == nil || !sameIncidentClosure(*c.pendingClosure, effect.Closure) || c.pendingClosure.Frozen {
		c.mu.Unlock()
		return fmt.Errorf("closure changed before residual handoff")
	}
	closure := *cloneIncidentClosure(c.pendingClosure)
	_, err := c.stepClosureLocked(lifecyclecore.Event{Kind: lifecyclecore.Reobserve, At: c.clock()})
	if err != nil {
		c.mu.Unlock()
		return err
	}
	c.incidentOpen = true
	c.currentIncidentRequest = cloneIncidentRequest(&closure.Request)
	c.currentIncidentResult = cloneIncidentResult(closure.Result)
	c.incidentDetectedAt = closure.DetectedAt
	c.incidentDispatchedAt = closure.DispatchedAt
	c.responderCompletedAt = closure.ResponderCompletedAt
	c.responderDone = true
	c.dispatchState = contracts.DispatchState_DISPATCH_STATE_COMPLETED
	c.verificationDeadline = c.closureVerificationDeadline(closure)
	c.incidentFindingKeys = findingKeys(closure.Request.Findings)
	c.cleanedHelpers = append([]string(nil), closure.CleanedHelpers...)
	c.timeline = cloneTimeline(closure.DetectorTimeline)
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	c.mu.Unlock()
	return c.PersistState(ctx)
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
