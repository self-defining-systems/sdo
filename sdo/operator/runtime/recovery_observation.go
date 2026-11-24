package runtime

import (
	"context"
	"fmt"
	"slices"
	"time"

	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

type recoveryObservation struct {
	ID       string
	Findings []sdk.Finding
	Status   DetectorEvaluationStatus
	Error    error
	Reason   string
}

// alertSetHealthDetectors are the health detectors among the detectors
// Recovered waits on (recoveryDetectorSet). Empty without health detectors.
func (c *Controller) alertSetHealthDetectors(request IncidentRequest) []string {
	var ids []string
	for _, id := range c.recoveryDetectorIDs(request) {
		if slices.Contains(c.healthDetectorIDs, id) {
			ids = append(ids, id)
		}
	}
	return ids
}

// recoveryDetectorIDs is the health authority for both recovery publication and
// the final live recheck: the shared recoveryDetectorSet over the detectors the
// controller evaluates.
func (c *Controller) recoveryDetectorIDs(request IncidentRequest) []string {
	known := append([]string(nil), c.healthDetectorIDs...)
	for id := range c.detectors {
		known = append(known, id)
	}
	return recoveryDetectorSet(request, known, c.healthDetectorIDs)
}

// Clearing persistence belongs to the same health authority at publication
// and broker handoff. It overlaps the raw clear streak; it never starts a
// second stability clock. Caller holds c.mu.
func (c *Controller) recoveryTrackersClear(request IncidentRequest) bool {
	if len(c.healthDetectorIDs) == 0 {
		return !c.tracker.HasActiveKeys(alertKeys(&request))
	}
	scope := newRecoveryScope(alertKeys(&request))
	return len(scope.blocking(c.tracker.ActiveFindings(c.healthDetectorIDs, nil))) == 0
}

func validateDetectorObservation(detector sdk.Detector, snapshot sdk.DetectionContext, findings []sdk.Finding, err error, scope recoveryScope) ([]sdk.Finding, DetectorEvaluationStatus, error) {
	if err != nil {
		return nil, DetectorEvaluationError, err
	}
	findings = append([]sdk.Finding(nil), findings...)
	status := DetectorEvaluationClear
	for i := range findings {
		f := &findings[i]
		if f.DetectorID == "" {
			f.DetectorID = detector.Spec().ID
		}
		if len(f.Playbooks) == 0 {
			f.Playbooks = append([]string(nil), detector.Spec().Playbooks...)
		}
		f.Fingerprint = FindingFingerprint(*f)
		if err := core.ValidateFinding(detector.Spec(), *f); err != nil {
			return nil, DetectorEvaluationError, err
		}
		if f.Status == sdk.FindingActive {
			status = DetectorEvaluationFiring
		}
	}
	if status == DetectorEvaluationClear && !detectorConfirmedIn(detector, snapshot, scope) {
		status = DetectorEvaluationPending
	}
	return findings, status, nil
}

// A closure must not use a scheduled green from an older snapshot. Samples
// already taken by this controller step are reused; persistence is never counted
// twice for one observation. Call under observationMu, outside c.mu.
func (c *Controller) observeRecoveryHealth(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, request IncidentRequest) {
	var pending []string
	var detectors []sdk.Detector
	for _, id := range c.recoveryDetectorIDs(request) {
		states, _ := c.latestEvaluations([]string{id}, time.Time{}, false)
		if len(states) > 0 && states[0].EvaluatedAt.Equal(now) {
			continue
		}
		detector := c.detectors[id]
		if detector == nil {
			c.recordDetectorError(id, now, fmt.Errorf("health detector unavailable"))
			continue
		}
		pending = append(pending, id)
		detectors = append(detectors, detector)
	}
	scope := c.recoveryScope()
	reads := readRecoveryDetectors(ctx, detectors, snapshot, scope)
	for i, id := range pending {
		read := reads[id]
		c.recordRecoveryObservation(ctx, id, now, read.findings, read.status, read.err, detectorConfirmationReasonIn(detectors[i], snapshot, scope), scope)
	}
}

func (c *Controller) recordRecoveryObservation(ctx context.Context, id string, now time.Time, findings []sdk.Finding, status DetectorEvaluationStatus, err error, reason string, scope recoveryScope) {
	states, _ := c.latestEvaluations([]string{id}, time.Time{}, false)
	if len(states) > 0 && states[0].EvaluatedAt.Equal(now) && states[0].Status == status {
		return
	}
	if err != nil {
		c.recordDetectorError(id, now, err)
		return
	}
	if status == DetectorEvaluationPending {
		c.history = append(c.history, DetectorEvaluation{DetectorID: id, EvaluatedAt: now, Status: status, Reason: reason})
		c.trackClearStreak(id, now, false)
	} else {
		changes := c.tracker.Observe(now, id, findings)
		c.batcher.RemoveKeys(changes.Cleared)
		c.recordEvaluation(id, now, findings, scope)
		if detector := c.detectors[id]; detector != nil {
			spec := detector.Spec()
			c.recordFiring(spec, now, findings, changes)
			for _, finding := range changes.Activated {
				c.admitActivation(ctx, finding, now, spec)
			}
			c.admitUpgrades(ctx, changes.Reruled, now, spec)
		}
	}
	if len(c.history) > 100 {
		c.history = append([]DetectorEvaluation(nil), c.history[len(c.history)-100:]...)
	}
}
