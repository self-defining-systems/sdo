package runtime

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
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

// alertSetHealthDetectors are the health detectors that own the incident's
// alerts (opening and joined). Recovered depends only on the alert set, so a
// health detector that owns none of them (a probe pending for lack of samples,
// a reader unavailable) never holds recovery. An incident whose alerts name no
// health detector waits on all of them.
func (c *Controller) alertSetHealthDetectors(request IncidentRequest) []string {
	owners := map[string]bool{}
	for _, key := range alertKeys(&request) {
		if id, _, ok := strings.Cut(key, "\x00"); ok {
			owners[id] = true
		}
	}
	var scoped []string
	for _, id := range c.healthDetectorIDs {
		if owners[id] {
			scoped = append(scoped, id)
		}
	}
	if len(scoped) == 0 {
		return append([]string(nil), c.healthDetectorIDs...)
	}
	return scoped
}

// recoveryDetectorIDs is the health authority for both recovery publication and
// the final live recheck. Legacy bundles without health use incident detectors.
func (c *Controller) recoveryDetectorIDs(request IncidentRequest) []string {
	ids := c.alertSetHealthDetectors(request)
	if len(ids) == 0 {
		for _, f := range request.Findings {
			if !strings.HasPrefix(f.DetectorID, "drift.") {
				ids = append(ids, f.DetectorID)
			}
		}
	}
	sort.Strings(ids)
	return slices.Compact(ids)
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
