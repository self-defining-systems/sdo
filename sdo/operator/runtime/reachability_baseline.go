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

// errBaselineDeferred marks a refresh that could not be authorized now. The old
// record stays and the next gate tries again.
var errBaselineDeferred = errors.New("reachability baseline refresh deferred")

// ReachabilityReference is this cluster's reachability baseline together with
// the accepted health inputs the detector compares it against. The startup gate
// replaces it in place, so a running controller judges against its own new
// record without a restart. The zero value holds no record.
type ReachabilityReference struct {
	mu                            sync.RWMutex
	baseline                      *servicehealth.ReachabilityBaseline
	sourceInputs, objectiveDigest string
}

func (r *ReachabilityReference) set(b *servicehealth.ReachabilityBaseline, revision memory.HealthRevision) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.baseline, r.sourceInputs, r.objectiveDigest = b, revision.SourceInputs, objectiveDigest(revision.Objective)
}

func (r *ReachabilityReference) bind(w *servicehealth.ReachabilityWindow) {
	if r == nil {
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	w.Baseline, w.SourceInputs, w.ObjectiveDigest = r.baseline, r.sourceInputs, r.objectiveDigest
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
		return nil, fmt.Errorf("%w: %w", errBaselineDeferred, err)
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
// startup and on resume, so each cluster measures against the accepted head it
// runs and never shares the record with another cluster.
func acceptStartupReachabilityBaseline(ctx context.Context, c *Controller, root, stateRoot string, enabled bool, observer *ServiceReachabilityObserver, provider SnapshotProvider, reference *ReachabilityReference, detectors []sdk.Detector) error {
	if !enabled || stateRoot == "" || observer == nil || reference == nil {
		return nil
	}
	c.mu.Lock()
	busy := c.incidentOpen || c.currentIncidentRequest != nil || c.closureBlocksNextLocked()
	c.mu.Unlock()
	if busy {
		return nil
	}
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
		return c.deferStaleBaseline(ctx, existing, revision, fmt.Errorf("reachability healthy baseline not accepted: %w", err))
	}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	baseline, err := captureReachabilityBaseline(ctx, stateRoot, existing, revision, detectors, snapshot)
	if err != nil {
		if errors.Is(err, errBaselineDeferred) {
			return c.deferStaleBaseline(ctx, existing, revision, err)
		}
		return err
	}
	reference.set(baseline, revision)
	return nil
}

// deferStaleBaseline keeps a record that is current except for a stale exclusion
// when its refresh cannot be authorized now (the goal is firing, the application
// is still changing). Failing the controller would crash-loop until the goal heals,
// and the same record is consumed meanwhile exactly as before this refresh existed.
// Every other record, and a cancelled context, still fails the gate.
func (c *Controller) deferStaleBaseline(ctx context.Context, existing *servicehealth.ReachabilityBaseline, revision memory.HealthRevision, cause error) error {
	if ctx.Err() != nil || !stalenessOnly(existing, revision) {
		return cause
	}
	if c.OnError != nil {
		c.OnError(fmt.Errorf("%w; keeping the recorded exclusions and retrying at the next gate", cause))
	}
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
