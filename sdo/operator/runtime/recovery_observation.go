package runtime

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// recoverySample records the scope of a scheduled or on-demand sample so a
// same-step join cannot reuse a narrower confirmation.
type recoverySample struct {
	at    time.Time
	scope recoveryScope
}

func (c *Controller) rememberRecoverySample(id string, at time.Time, scope recoveryScope) {
	if c.recoverySamples == nil {
		c.recoverySamples = make(map[string]recoverySample)
	}
	c.recoverySamples[id] = recoverySample{at, scope}
}

func (s recoverySample) matches(at time.Time, scope recoveryScope) bool {
	return s.at.Equal(at) && (s.scope == nil) == (scope == nil) && maps.Equal(s.scope, scope)
}

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
// a reader unavailable) never holds recovery.
func (c *Controller) alertSetHealthDetectors(request IncidentRequest) []string {
	owners := alertDetectorOwners(request)
	var scoped []string
	for _, id := range c.healthDetectorIDs {
		if owners[id] {
			scoped = append(scoped, id)
		}
	}
	sort.Strings(scoped)
	return scoped
}

func alertDetectorOwners(request IncidentRequest) map[string]bool {
	owners := map[string]bool{}
	for _, key := range alertKeys(&request) {
		if id, _, ok := strings.Cut(key, "\x00"); ok {
			owners[id] = true
		}
	}
	return owners
}

// recoveryDetectorIDs includes the alert owners for legacy bundles without
// classified health detectors, using the same key scope as current bundles.
func (c *Controller) recoveryDetectorIDs(request IncidentRequest) []string {
	if len(c.healthDetectorIDs) > 0 {
		return c.alertSetHealthDetectors(request)
	}
	var ids []string
	for id := range alertDetectorOwners(request) {
		if !strings.HasPrefix(id, "drift.") {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// Tracker persistence holds only for alert-set members.
func (c *Controller) recoveryTrackersClear(request IncidentRequest) bool {
	return !c.tracker.HasActiveKeys(alertKeys(&request))
}

func validateDetectorObservation(detector sdk.Detector, snapshot sdk.DetectionContext, findings []sdk.Finding, err error, scope recoveryScope) ([]sdk.Finding, DetectorEvaluationStatus, error) {
	if err != nil {
		return nil, DetectorEvaluationError, err
	}
	findings, err = admitFindings(detector.Spec(), findings)
	if err != nil {
		return nil, DetectorEvaluationError, err
	}
	status := DetectorEvaluationClear
	for _, f := range findings {
		if f.Status == sdk.FindingActive && scope.holds(f) {
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
	scope := newRecoveryScope(alertKeys(&request))
	var pending []string
	var detectors []sdk.Detector
	for _, id := range c.recoveryDetectorIDs(request) {
		if c.recoverySamples[id].matches(now, scope) {
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
	reads := readRecoveryDetectors(ctx, detectors, snapshot, scope)
	for i, id := range pending {
		read := reads[id]
		c.recordRecoveryObservation(ctx, id, now, read.findings, read.status, read.err, detectorConfirmationReasonIn(detectors[i], snapshot, scope), scope)
	}
}

func (c *Controller) recordRecoveryObservation(ctx context.Context, id string, now time.Time, findings []sdk.Finding, status DetectorEvaluationStatus, err error, reason string, scope recoveryScope) {
	states, _ := c.latestEvaluations([]string{id}, time.Time{}, false)
	if c.recoverySamples[id].matches(now, scope) {
		return
	}
	c.rememberRecoverySample(id, now, scope)
	if err != nil {
		c.recordDetectorError(id, now, err)
		return
	}
	if status == DetectorEvaluationPending {
		c.history = append(c.history, DetectorEvaluation{DetectorID: id, EvaluatedAt: now, Status: status, Reason: reason})
		c.trackClearStreak(id, now, false)
		c.observeAvailableFindings(ctx, id, now, findings, scope)
	} else if len(states) > 0 && states[0].EvaluatedAt.Equal(now) && states[0].Status != DetectorEvaluationPending && states[0].Status != DetectorEvaluationError {
		// The tracker already consumed this step's raw findings. Only the
		// scope of their recovery confirmation changed.
		c.recordEvaluation(id, now, findings, scope)
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

// An unconfirmed alert cannot clear from silence. An independently firing
// sibling still feeds admission and can join the incident.
func (c *Controller) observeAvailableFindings(ctx context.Context, id string, now time.Time, findings []sdk.Finding, scope recoveryScope) {
	if len(findings) == 0 || scope == nil {
		return
	}
	detector := c.detectors[id]
	if detector == nil {
		return
	}
	changes := c.tracker.observe(now, id, findings, "", scope)
	c.batcher.RemoveKeys(changes.Cleared)
	spec := detector.Spec()
	c.recordFiring(spec, now, findings, changes)
	for _, finding := range changes.Activated {
		c.admitActivation(ctx, finding, now, spec)
	}
	c.admitUpgrades(ctx, changes.Reruled, now, spec)
}
