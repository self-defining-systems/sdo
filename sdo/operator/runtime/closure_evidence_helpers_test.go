package runtime

import (
	"context"
	"time"
)

// settleClosureEvidence finishes a hand-stepped controller's closure. A pass
// that verifies an incident gathers its evidence off the step loop and returns;
// the run loop makes the next pass when the evidence is ready. Tests that step
// by hand do the same: wait for the evidence, then step again at the same
// instant so the pass consumes it.
func settleClosureEvidence(ctx context.Context, c *Controller, now time.Time) error {
	c.mu.Lock()
	gathering := c.closurePrep != nil
	c.mu.Unlock()
	if !gathering {
		return nil
	}
	<-c.ClosurePrepared()
	return c.Step(ctx, now, nil)
}

// stepSettled is Step followed by settleClosureEvidence.
func stepSettled(ctx context.Context, c *Controller, now time.Time) error {
	if err := c.Step(ctx, now, nil); err != nil {
		return err
	}
	return settleClosureEvidence(ctx, c, now)
}
