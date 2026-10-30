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

// recoveryDetectorIDs is the health authority for both recovery publication and
// the final live recheck. Legacy bundles without health use incident detectors.
func (c *Controller) recoveryDetectorIDs(request IncidentRequest) []string {
	ids := append([]string(nil), c.healthDetectorIDs...)
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

// readRecoveryDetector is shared by the proof producer and closure consumer.
// A successful transport read alone cannot turn incomplete observations green.
func readRecoveryDetector(ctx context.Context, detector sdk.Detector, snapshot sdk.DetectionContext) ([]sdk.Finding, DetectorEvaluationStatus, error) {
	findings, err := sdk.DetectObserved(ctx, detector, snapshot)
	if err != nil {
		return nil, DetectorEvaluationError, err
	}
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
	if status == DetectorEvaluationClear && !detectorConfirmed(detector, snapshot) {
		status = DetectorEvaluationPending
	}
	return findings, status, nil
}

// A closure must not use a scheduled green from an older snapshot. Samples
// already taken by this controller step are reused; persistence is never counted
// twice for one observation. Call under observationMu, outside c.mu.
func (c *Controller) observeRecoveryHealth(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, request IncidentRequest) {
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
		findings, status, err := readRecoveryDetector(ctx, detector, snapshot)
		c.recordRecoveryObservation(id, now, findings, status, err, detectorConfirmationReason(detector, snapshot))
	}
}

func (c *Controller) recordRecoveryObservation(id string, now time.Time, findings []sdk.Finding, status DetectorEvaluationStatus, err error, reason string) {
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
		c.recordEvaluation(id, now, findings)
	}
	if len(c.history) > 100 {
		c.history = append([]DetectorEvaluation(nil), c.history[len(c.history)-100:]...)
	}
}
