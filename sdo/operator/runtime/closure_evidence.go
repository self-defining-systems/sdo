package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

// closureEvidenceInput is the verified incident as it stood when its health
// proof passed. Everything a closure's slow evidence needs is copied here, so
// the work reads no controller state and may run while detection continues.
type closureEvidenceInput struct {
	IncidentID           string
	Findings             []sdk.Finding
	Snapshot             sdk.DetectionContext
	Loop                 *ResponderLoop
	VerifiedAt           time.Time
	ResponderCompletedAt time.Time
	// Result and Stages are set when settlement applies: the responder is done
	// and its loop can refresh the stage evidence.
	Result *IncidentResult
	Stages []*contracts.StageReport
}

// closureEvidence is what the slow work adds to the closure.
type closureEvidence struct {
	Settled bool
	Result  IncidentResult
	Stages  []*contracts.StageReport
}

// closureEvidenceNeeded reports whether verifying this incident has slow
// evidence to gather: corpus captures, and settlement of a finished responder.
func (c *Controller) closureEvidenceNeeded(in closureEvidenceInput) bool {
	return c.config.CorpusSessionRoot != "" || in.Result != nil && in.Loop != nil
}

// gatherClosureEvidence captures the corpus and settles recovery applications.
// It touches no controller state, so it is safe to run off the step loop.
func (c *Controller) gatherClosureEvidence(ctx context.Context, in closureEvidenceInput) (closureEvidence, error) {
	if c.closureEvidence != nil {
		return c.closureEvidence(ctx, in)
	}
	evidence := closureEvidence{}
	if err := c.captureClosureEvidence(ctx, in); err != nil {
		return evidence, err
	}
	if in.Result != nil && in.Loop != nil {
		evidence.Settled = true
		evidence.Result, evidence.Stages = in.Loop.SettleRecoveryApplications(ctx, *in.Result, in.Stages, in.VerifiedAt)
	}
	return evidence, nil
}

// captureClosureEvidence stores the verified snapshot, binds the healthy
// baseline, and replays the open-phase program reads against the repaired
// cluster. The cost is reported because these reads are the slow part.
func (c *Controller) captureClosureEvidence(ctx context.Context, in closureEvidenceInput) error {
	root := c.config.CorpusSessionRoot
	if root == "" {
		return nil
	}
	start := c.clock()
	if err := memory.CaptureIncidentSnapshot(root, in.IncidentID, "fixed", in.Snapshot, c.config.Namespace, findingCaptureRefs(in.Findings)...); err != nil {
		return fmt.Errorf("capture verified snapshot: %w", err)
	}
	if err := memory.BindClosureHealthyBaseline(root, in.IncidentID); err != nil {
		return fmt.Errorf("bind closure healthy baseline: %w", err)
	}
	snapshotMs := c.clock().Sub(start).Milliseconds()
	if c.drift == nil || c.drift.View == nil {
		return nil
	}
	readsStart := c.clock()
	err := memory.CaptureRepairedProgramReads(root, in.IncidentID, boundedProbeView{c.drift.View(in.Snapshot)})
	readsMs := c.clock().Sub(readsStart).Milliseconds()
	routingStart := c.clock()
	if err == nil {
		err = captureRoutingEvidence(ctx, root, in.IncidentID, "fixed", in.Findings, c.drift.View(in.Snapshot))
	}
	slog.Info("verified_capture_timing", "incident_id", in.IncidentID, "snapshot_ms", snapshotMs, "program_reads_ms", readsMs, "routing_ms", c.clock().Sub(routingStart).Milliseconds())
	if err != nil {
		return fmt.Errorf("capture verified program reads: %w", err)
	}
	return nil
}

// closurePreparation is the evidence of one verified incident being gathered
// off the step loop. It belongs to the incident and responder completion it was
// started for; any other pass ignores and replaces it. Guarded by Controller.mu.
type closurePreparation struct {
	incidentID           string
	responderCompletedAt time.Time
	done                 bool
	evidence             closureEvidence
	err                  error
}

func (p *closurePreparation) matches(in closureEvidenceInput) bool {
	return p.incidentID == in.IncidentID && p.responderCompletedAt.Equal(in.ResponderCompletedAt)
}

// closurePreparingLocked reports that the open incident's closure evidence is
// still being gathered, so a pass has nothing to decide yet. Caller holds c.mu.
func (c *Controller) closurePreparingLocked() bool {
	return c.closurePrep != nil && !c.closurePrep.done && c.currentIncidentRequest != nil &&
		c.closurePrep.incidentID == c.currentIncidentRequest.IncidentID
}

// ClosurePrepared is signalled when background closure evidence finishes, so the
// run loop makes the pass that freezes the closure without waiting for a timer.
func (c *Controller) ClosurePrepared() <-chan struct{} { return c.closurePrepared }

// closureEvidenceLocked starts gathering the evidence on a goroutine and
// returns not-ready; a later pass consumes it once it is done. The step loop and
// observation lock are never held across the slow reads, so detectors keep
// evaluating. Nothing is frozen or published until a pass consumes the evidence
// and re-proves health, so the frozen closure still reflects one consistent,
// freshly verified observation. Caller holds c.mu, which is held on return.
func (c *Controller) closureEvidenceLocked(ctx context.Context, in closureEvidenceInput) (*closureEvidence, bool) {
	prep := c.closurePrep
	if prep == nil || !prep.matches(in) {
		prep = &closurePreparation{incidentID: in.IncidentID, responderCompletedAt: in.ResponderCompletedAt}
		c.closurePrep = prep
		c.goBackground(func() {
			evidence, err := c.gatherClosureEvidence(ctx, in)
			c.mu.Lock()
			prep.done, prep.evidence, prep.err = true, evidence, err
			c.mu.Unlock()
			select {
			case c.closurePrepared <- struct{}{}:
			default:
			}
		})
		return nil, false
	}
	if !prep.done {
		return nil, false
	}
	c.closurePrep = nil
	if prep.err != nil {
		// The next pass gathers again, as a failed capture always did.
		c.mu.Unlock()
		c.reportBrokerError("gather closure evidence", prep.err)
		c.mu.Lock()
		return nil, false
	}
	evidence := prep.evidence
	return &evidence, true
}

// observationHoldWarn is how long a pass may hold the observation lock before
// the hold is logged. While it is held no detector evaluates, so a long holder
// is a continuity defect and must name itself.
const observationHoldWarn = time.Second

// lockObservation takes observationMu for the named holder and returns its
// release. Every holder goes through it so a stall names its cause.
func (c *Controller) lockObservation(holder string) func() {
	c.observationMu.Lock()
	start := c.clock()
	return func() {
		held := c.clock().Sub(start)
		c.observationMu.Unlock()
		if held >= observationHoldWarn {
			slog.Warn("observation_lock_held", "holder", holder, "held_ms", held.Milliseconds())
		}
	}
}
