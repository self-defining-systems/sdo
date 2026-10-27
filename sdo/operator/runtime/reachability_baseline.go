package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
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

// baselineProposalID is unique per accepted revision and measured configuration.
// A configuration that returns to an earlier value (R, C, R) at a later commit
// never reuses the first proposal.
func baselineProposalID(revision memory.HealthRevision, b *servicehealth.ReachabilityBaseline) string {
	digest := sha256.New()
	for _, e := range b.Exclusions {
		fmt.Fprintf(digest, "%s\x00%s\x00", e.Target.ID(), e.Configuration)
	}
	return fmt.Sprintf("reachability-baseline-%s-%.12s-%x", revision.RefreshID(), revision.Commit, digest.Sum(nil)[:6])
}

// publishReachabilityBaseline is reached only from the trusted, opt-in startup
// baseline gate before dispatch. It writes an isolated health-judge proposal and
// uses the ordinary broker ownership/validation path. The accepted checkout is
// never directly edited. A current record, including an empty one, is retained.
func publishReachabilityBaseline(ctx context.Context, root string, authority *broker.Broker, revision memory.HealthRevision, detectors []sdk.Detector, snapshot sdk.DetectionContext) (bool, error) {
	existing, err := memory.ReadReachabilityBaseline(ctx, root, revision.Commit)
	if err != nil {
		return false, err
	}
	if baselineMatches(existing, revision, snapshot) {
		return false, nil
	}
	if _, err := healthyBaselineGoal(ctx, detectors, snapshot); err != nil {
		return false, fmt.Errorf("%w: %w", errBaselineDeferred, err)
	}
	baseline, err := servicehealth.CaptureReachabilityBaseline(snapshot, servicehealth.BaselineAuthorization{SourceInputs: revision.SourceInputs, ObjectiveDigest: objectiveDigest(revision.Objective), Reason: "healthy-baseline", GoalHealthy: true})
	if err != nil {
		return false, err
	}
	if stalenessOnly(existing, revision) {
		keepRecordedExclusions(baseline, existing)
	}
	id := baselineProposalID(revision, baseline)
	proposal, err := authority.CreateProposalForPhase(ctx, id, broker.InitialPhase)
	if err != nil {
		return false, err
	}
	if proposal.BaseCommit != revision.Commit {
		return false, fmt.Errorf("healthy baseline accepted inputs advanced during measurement")
	}
	if err := memory.WriteReachabilityBaseline(proposal.Path, baseline); err != nil {
		return false, err
	}
	receipt, err := authority.Publish(ctx, id, broker.HealthJudge, broker.InitialPhase)
	if err != nil {
		return false, err
	}
	return receipt.Commit != "", nil
}

// acceptStartupReachabilityBaseline requires the existing explicit healthy-
// baseline opt-in, fresh probes, and independent goal checks throughout capture.
// A restored investigation/active closure can only consume accepted data, never learn
// an exclusion from its incident state. Publication asks the builder to adopt
// the new accepted diagnostic revision before any incident can start.
func acceptStartupReachabilityBaseline(ctx context.Context, c *Controller, root string, authority *broker.Broker, enabled bool, observer *ServiceReachabilityObserver, provider SnapshotProvider, detectors []sdk.Detector) error {
	if !enabled || authority == nil || observer == nil {
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
	existing, err := memory.ReadReachabilityBaseline(ctx, root, revision.Commit)
	if err != nil {
		return err
	}
	current, err := provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	if baselineMatches(existing, revision, current) {
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
	accepted, err := publishReachabilityBaseline(ctx, root, authority, revision, detectors, snapshot)
	if err != nil {
		if errors.Is(err, errBaselineDeferred) {
			return c.deferStaleBaseline(ctx, existing, revision, err)
		}
		return err
	}
	if accepted {
		return memory.ErrHealthRefreshRequired
	}
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

// loadReachabilityBaseline binds optional accepted data without requiring a
// health repository from callers that only use the low-level runtime API.
func loadReachabilityBaseline(ctx context.Context, root, commit string) (*servicehealth.ReachabilityBaseline, string, string, error) {
	b, err := memory.ReadReachabilityBaseline(ctx, root, commit)
	if err != nil || b == nil {
		return b, "", "", err
	}
	revision, err := memory.HealthInputs(ctx, root, commit)
	if err != nil {
		return nil, "", "", err
	}
	return b, revision.SourceInputs, objectiveDigest(revision.Objective), nil
}
