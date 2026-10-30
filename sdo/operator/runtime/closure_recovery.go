package runtime

import (
	"context"
	"fmt"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/lifecyclecore"
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
	c.closureStableSince = time.Time{}
	c.recoverySafetyLost = true
	c.mu.Unlock()
	return c.PersistState(ctx)
}
