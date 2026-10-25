package runtime

import (
	"context"
	"crypto/sha256"
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

func baselineMatches(b *servicehealth.ReachabilityBaseline, revision memory.HealthRevision) bool {
	return b != nil && b.Authorization.SourceInputs == revision.SourceInputs && b.Authorization.ObjectiveDigest == objectiveDigest(revision.Objective)
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
	if baselineMatches(existing, revision) {
		return false, nil
	}
	if _, err := healthyBaselineGoal(ctx, detectors, snapshot); err != nil {
		return false, err
	}
	baseline, err := servicehealth.CaptureReachabilityBaseline(snapshot, servicehealth.BaselineAuthorization{SourceInputs: revision.SourceInputs, ObjectiveDigest: objectiveDigest(revision.Objective), Reason: "healthy-baseline", GoalHealthy: true})
	if err != nil {
		return false, err
	}
	id := "reachability-baseline-" + revision.RefreshID()
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
	if baselineMatches(existing, revision) {
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
		return fmt.Errorf("reachability healthy baseline not accepted: %w", err)
	}
	snapshot, err := provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	accepted, err := publishReachabilityBaseline(ctx, root, authority, revision, detectors, snapshot)
	if err != nil {
		return err
	}
	if accepted {
		return memory.ErrHealthRefreshRequired
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
