package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sync"
	"time"
)

func objectiveDigest(objective string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(objective)))
}

// healthyBaselineGoal is independent of the generic reachability policy. No
// finding, error, missing objective, or incident detector can authorize a baseline.
func healthyBaselineGoal(ctx context.Context, detectors []sdk.Detector, snapshot sdk.DetectionContext) (bool, error) {
	objective := false
	for _, detector := range detectors {
		spec := detector.Spec()
		if spec.Class != sdk.DetectorClassHealth {
			continue
		}
		if spec.ID == "service-reachability" {
			continue
		}
		if spec.ID == "health-objective" {
			objective = true
		}
		findings, err := sdk.DetectObserved(ctx, detector, snapshot)
		if err != nil {
			return false, fmt.Errorf("healthy baseline %s: %w", spec.ID, err)
		}
		for _, finding := range findings {
			if finding.Status == sdk.FindingActive {
				return false, fmt.Errorf("healthy baseline %s is firing", spec.ID)
			}
		}
	}
	if !objective {
		return false, fmt.Errorf("healthy baseline requires an independently evaluated health-objective")
	}
	return true, nil
}

// baselineMatches reports whether the record is current: bound to the accepted
// health inputs, and every exclusion still describes its target's configuration.
func baselineMatches(b *servicehealth.ReachabilityBaseline, revision memory.HealthRevision, snapshot sdk.DetectionContext) bool {
	return b != nil && b.Authorization.SourceInputs == revision.SourceInputs && b.Authorization.ObjectiveDigest == objectiveDigest(revision.Objective) && len(b.StaleExclusions(snapshot)) == 0
}

// stalenessOnly reports a record bound to the accepted health inputs whose only
// defect is a stale exclusion. Such a record may be measured again, but only for
// the targets it already excludes.
func stalenessOnly(b *servicehealth.ReachabilityBaseline, revision memory.HealthRevision) bool {
	return b != nil && b.Authorization.SourceInputs == revision.SourceInputs && b.Authorization.ObjectiveDigest == objectiveDigest(revision.Objective)
}

// keepRecordedExclusions drops every exclusion whose target the earlier record does
// not already exclude. A refresh that staleness alone triggered re-measures recorded
// targets; a port that newly fails is a defect to report, never an exclusion.
func keepRecordedExclusions(b, earlier *servicehealth.ReachabilityBaseline) {
	recorded := map[string]bool{}
	for _, e := range earlier.Exclusions {
		recorded[e.Namespace+"/"+e.Target.ID()] = true
	}
	kept := b.Exclusions[:0]
	for _, e := range b.Exclusions {
		if recorded[e.Namespace+"/"+e.Target.ID()] {
			kept = append(kept, e)
		}
	}
	b.Exclusions = kept
}

// While no current record is bound, the gate checks the goal again after
// reachabilityBaselineCheck. A measurement that ran and failed blocks the
// controller loop for its probe rounds, so it is repeated only after
// reachabilityBaselineRetry.
const (
	reachabilityBaselineCheck = 5 * time.Second
	reachabilityBaselineRetry = 30 * time.Second
)

// errBaselineNotAuthorized marks a goal that refused before any probe ran.
var errBaselineNotAuthorized = errors.New("healthy baseline goal not met")

// BaselineUnavailableError says the gate could not bind a current baseline. It is
// a state, never a controller failure: the record is recomputable, so the
// controller keeps observing, reports never-served targets unknown, and measures
// again once the application is healthy.
type BaselineUnavailableError struct{ Cause error }

func (e *BaselineUnavailableError) Error() string {
	return fmt.Sprintf("reachability baseline unavailable; never-served targets are unknown until a healthy measurement: %v", e.Cause)
}

func (e *BaselineUnavailableError) Unwrap() error { return e.Cause }

// ReachabilityReference is this cluster's reachability baseline together with
// the accepted health inputs the detector compares it against. The gate
// replaces it in place, so a running controller judges against its own new
// record without a restart. The zero value holds no record.
type ReachabilityReference struct {
	mu                            sync.RWMutex
	baseline                      *servicehealth.ReachabilityBaseline
	sourceInputs, objectiveDigest string
	unavailable                   *BaselineUnavailableError
	retry                         time.Time
}

func (r *ReachabilityReference) set(b *servicehealth.ReachabilityBaseline, revision memory.HealthRevision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.baseline, r.sourceInputs, r.objectiveDigest = b, revision.SourceInputs, objectiveDigest(revision.Objective)
	r.unavailable, r.retry = nil, time.Time{}
}

// await keeps whatever record is bound (its still-current exclusions apply),
// marks the baseline unavailable, and schedules the next attempt.
func (r *ReachabilityReference) await(cause error, retry time.Time) *BaselineUnavailableError {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unavailable, r.retry = &BaselineUnavailableError{Cause: cause}, retry
	return r.unavailable
}

// postpone moves a due attempt the gate skipped (an incident is open) to retry.
func (r *ReachabilityReference) postpone(retry time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unavailable != nil {
		r.retry = retry
	}
}

// Unavailable is why no current baseline is bound, or nil.
func (r *ReachabilityReference) Unavailable() *BaselineUnavailableError {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.unavailable
}

// retryAt is when the gate should measure again, or zero when a current record is bound.
func (r *ReachabilityReference) retryAt() time.Time {
	if r == nil {
		return time.Time{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.unavailable == nil {
		return time.Time{}
	}
	return r.retry
}

func (r *ReachabilityReference) bind(w *servicehealth.ReachabilityWindow) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	w.Baseline, w.SourceInputs, w.ObjectiveDigest = r.baseline, r.sourceInputs, r.objectiveDigest
	w.BaselinePending = r.unavailable != nil
}

// captureReachabilityBaseline is reached only from the trusted, opt-in startup
// baseline gate before dispatch. It writes this cluster's controller state; the
// accepted repository is never touched. A current record, including an empty
// one, is retained.
func captureReachabilityBaseline(ctx context.Context, stateRoot string, existing *servicehealth.ReachabilityBaseline, revision memory.HealthRevision, detectors []sdk.Detector, snapshot sdk.DetectionContext) (*servicehealth.ReachabilityBaseline, error) {
	if baselineMatches(existing, revision, snapshot) {
		return existing, nil
	}
	if _, err := healthyBaselineGoal(ctx, detectors, snapshot); err != nil {
		return nil, err
	}
	baseline, err := servicehealth.CaptureReachabilityBaseline(snapshot, servicehealth.BaselineAuthorization{SourceInputs: revision.SourceInputs, ObjectiveDigest: objectiveDigest(revision.Objective), Reason: "healthy-baseline", GoalHealthy: true})
	if err != nil {
		return nil, err
	}
	if stalenessOnly(existing, revision) {
		keepRecordedExclusions(baseline, existing)
	}
	if err := memory.WriteReachabilityBaselineState(stateRoot, baseline); err != nil {
		return nil, err
	}
	return baseline, nil
}

// acceptStartupReachabilityBaseline requires the existing explicit healthy-
// baseline opt-in, fresh probes, and independent goal checks throughout capture.
// A restored investigation/active closure can only consume the recorded state,
// never learn an exclusion from its incident state. It runs at controller
// startup, on resume, and while no current record is bound, so each cluster
// measures against the accepted head it runs and never shares the record with
// another cluster. It cannot fail the controller: when no current record can be
// measured (the goal is firing, the application is still changing, a read
// failed) the reference is marked unavailable and the next pass measures again.
func acceptStartupReachabilityBaseline(ctx context.Context, c *Controller, root, stateRoot string, enabled bool, observer *ServiceReachabilityObserver, provider SnapshotProvider, reference *ReachabilityReference, detectors []sdk.Detector) {
	if !enabled || stateRoot == "" || observer == nil || reference == nil {
		return
	}
	c.mu.Lock()
	busy := c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked()
	c.mu.Unlock()
	if busy {
		return
	}
	if err := measureReachabilityBaseline(ctx, c, root, stateRoot, observer, provider, reference, detectors); err != nil && ctx.Err() == nil {
		spacing := reachabilityBaselineRetry
		if errors.Is(err, errBaselineNotAuthorized) {
			spacing = reachabilityBaselineCheck
		}
		unavailable := reference.await(err, c.clock().Add(spacing))
		if c.OnError != nil {
			c.OnError(unavailable)
		}
	}
}

// retryReachabilityBaseline runs the gate again once a scheduled attempt is due.
func retryReachabilityBaseline(ctx context.Context, c *Controller, root, stateRoot string, enabled bool, observer *ServiceReachabilityObserver, provider SnapshotProvider, reference *ReachabilityReference, detectors []sdk.Detector) {
	now := c.clock()
	if at := reference.retryAt(); at.IsZero() || now.Before(at) {
		return
	}
	reference.postpone(now.Add(reachabilityBaselineCheck))
	acceptStartupReachabilityBaseline(ctx, c, root, stateRoot, enabled, observer, provider, reference, detectors)
}

func measureReachabilityBaseline(ctx context.Context, c *Controller, root, stateRoot string, observer *ServiceReachabilityObserver, provider SnapshotProvider, reference *ReachabilityReference, detectors []sdk.Detector) error {
	revision, err := memory.HealthInputs(ctx, root, "HEAD")
	if err != nil {
		return err
	}
	existing, err := memory.ReadReachabilityBaselineState(stateRoot)
	if err != nil {
		// Recomputable state: measure again rather than crash-loop on it.
		if c.OnError != nil {
			c.OnError(fmt.Errorf("discarding unreadable reachability baseline state: %w", err))
		}
		existing = nil
	}
	current, err := provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	if baselineMatches(existing, revision, current) {
		reference.set(existing, revision)
		return nil
	}
	// A firing goal cannot authorize a record; keep the observer's window.
	if _, err := healthyBaselineGoal(ctx, detectors, current); err != nil {
		return fmt.Errorf("%w: %w", errBaselineNotAuthorized, err)
	}
	observer.Stop()
	observer.Reset()
	defer observer.Start(ctx)
	// Collect invokes the independent application check after every fresh round.
	var configurations map[string]string
	if err := observer.collect(ctx, func() error {
		snapshot, err := provider.Snapshot(ctx)
		if err != nil {
			return err
		}
		current := map[string]string{}
		targets, _ := servicehealth.ReachabilityTargets(snapshot)
		for _, target := range targets {
			if config, ok := servicehealth.ReachabilityTargetConfiguration(snapshot, target); ok {
				current[target.ID()] = config
			}
		}
		if configurations != nil && !reflect.DeepEqual(configurations, current) {
			return fmt.Errorf("reachability target configuration changed during baseline measurement")
		}
		configurations = current
		_, err = healthyBaselineGoal(ctx, detectors, snapshot)
		return err
	}); err != nil {
		return fmt.Errorf("reachability healthy baseline not accepted: %w", err)
	}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	baseline, err := captureReachabilityBaseline(ctx, stateRoot, existing, revision, detectors, snapshot)
	if err != nil {
		return err
	}
	reference.set(baseline, revision)
	return nil
}

// loadReachabilityBaseline binds this cluster's recorded state, if any, to the
// accepted health inputs at commit. A legacy capture inside the accepted tree is
// never read: it belongs to whichever cluster wrote it. Unreadable state is
// recomputable, so it is reported and treated as no record.
func loadReachabilityBaseline(ctx context.Context, stateRoot, root, commit string, onError func(error)) (*ReachabilityReference, error) {
	reference := &ReachabilityReference{}
	b, err := memory.ReadReachabilityBaselineState(stateRoot)
	if err != nil && onError != nil {
		onError(fmt.Errorf("discarding unreadable reachability baseline state: %w", err))
	}
	if err != nil || b == nil {
		return reference, nil
	}
	revision, err := memory.HealthInputs(ctx, root, commit)
	if err != nil {
		return nil, err
	}
	reference.set(b, revision)
	return reference, nil
}
