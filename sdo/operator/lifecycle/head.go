package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// HeadBroker is the part of the broker a head deployment needs.
type HeadBroker interface {
	// HeadToDeploy syncs the branch and names the accepted head when its
	// effective source differs from the newest recorded deployment.
	HeadToDeploy(ctx context.Context) (change broker.HeadChange, needed bool, err error)
	AcceptedSourceEvidence(ctx context.Context) (accepted broker.AcceptedSource, found bool, err error)
	HeadCheckout(ctx context.Context, commit string) (path string, release func(), err error)
	RecordHeadDeployment(ctx context.Context, commit string, evidence broker.SourceEvidence) error
}

// HeadHolds remembers the heads the cluster or the detectors refused, so a
// broken source is not applied again on every poll. A hold is about the head's
// effective source: SDO's own memory commits on top of it do not supersede it.
type HeadHolds interface {
	// Held reports the hold on this effective source, if any.
	Held(ctx context.Context, change broker.HeadChange) (detail string, held bool, err error)
	Hold(ctx context.Context, change broker.HeadChange, reason HoldReason, detail string) error
}

// HeadOutcome is what Deploy did about the branch head.
type HeadOutcome string

const (
	// HeadCurrent: the head's source is what the newest deployment applied.
	HeadCurrent HeadOutcome = "current"
	// HeadDeployed: the head was applied, verified and recorded as the newest deployment.
	HeadDeployed HeadOutcome = "deployed"
	// HeadHeld: the cluster or the detectors refused the head (Hold and Detail
	// say why). Nothing is recorded and nothing is rolled back.
	HeadHeld HeadOutcome = "held"
	// HeadDeferred: the deploy did not start (an incident is open, or the
	// controller did not acknowledge the pause). Try again later.
	HeadDeferred HeadOutcome = "deferred"
)

// HeadResult is the typed result of one Deploy.
type HeadResult struct {
	Outcome HeadOutcome
	Commit  string
	Hold    HoldReason
	Detail  string
	Verdict HealthVerdict
}

// HeadDeployer deploys a branch head SDO did not write: an engineer's push,
// or the benchmark's replay. It is the repair deployer's path without a
// staged candidate: the commit already is the branch head, so success only
// records the deployment of exactly that commit. Only the newest head is
// deployed; intermediate heads are skipped.
type HeadDeployer struct {
	Broker HeadBroker
	// Deployer carries the readiness, the verifier, the maintenance pause,
	// the busy gate, the clock and the wait; its Broker is unused.
	Deployer RepairDeployer
	// Holds, when set, keeps refused heads from being retried.
	Holds HeadHolds
}

// Deploy deploys the branch head when it has not been deployed.
func (h HeadDeployer) Deploy(ctx context.Context, cfg RepairConfig) (HeadResult, error) {
	if h.Broker == nil {
		return HeadResult{}, errors.New("head deployment needs a broker")
	}
	if h.Deployer.Readiness == nil || h.Deployer.Verifier == nil || cfg.Namespace == "" || cfg.HealthObjective == "" {
		return HeadResult{}, errors.New("head deployment needs readiness, a verifier, a namespace and a health objective")
	}
	change, needed, err := h.Broker.HeadToDeploy(ctx)
	if err != nil {
		return HeadResult{}, err
	}
	if !needed {
		return HeadResult{Outcome: HeadCurrent}, nil
	}
	commit := change.Commit
	if h.Holds != nil {
		// Checked before the pause: a held head must not suspend detection on every poll.
		detail, held, err := h.Holds.Held(ctx, change)
		if err != nil {
			return HeadResult{}, err
		}
		if held {
			return HeadResult{Outcome: HeadHeld, Commit: commit, Detail: detail}, nil
		}
	}
	var out HeadResult
	paused, err := h.Deployer.paused(ctx, func() (RepairResult, error) {
		result, err := h.deploy(ctx, cfg, commit)
		out = result
		return RepairResult{}, err
	})
	if err != nil {
		return HeadResult{}, err
	}
	if paused.Outcome == RepairDeferred {
		return HeadResult{Outcome: HeadDeferred, Commit: commit, Detail: paused.Detail}, nil
	}
	if out.Outcome == HeadHeld && h.Holds != nil {
		if err := h.Holds.Hold(ctx, change, out.Hold, out.Detail); err != nil {
			return HeadResult{}, fmt.Errorf("record head hold: %w", err)
		}
	}
	return out, nil
}

func (h HeadDeployer) deploy(ctx context.Context, cfg RepairConfig, commit string) (HeadResult, error) {
	accepted, found, err := h.Broker.AcceptedSourceEvidence(ctx)
	if err != nil && !errors.Is(err, planfile.ErrAbsent) {
		return HeadResult{}, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if err != nil || !found || accepted.Plan.GetSource() == nil {
		return HeadResult{Outcome: HeadCurrent, Commit: commit, Detail: ErrNoAcceptedPlan.Error()}, nil
	}
	path, release, err := h.Broker.HeadCheckout(ctx, commit)
	if err != nil {
		return HeadResult{}, err
	}
	defer release()
	held := func(reason HoldReason, detail string) HeadResult {
		return HeadResult{Outcome: HeadHeld, Commit: commit, Hold: reason, Detail: detail}
	}
	steps := Coordinator{Wait: h.Deployer.Wait}
	bounds := Config{Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, PendingEvaluations: cfg.PendingEvaluations, PendingInterval: cfg.PendingInterval, InfrastructureTries: cfg.InfrastructureTries, InfrastructureBackoff: cfg.InfrastructureBackoff}
	var receipt DeploymentReceipt
	err = steps.retryInfrastructure(ctx, bounds, "head deployment", func() (err error) {
		receipt, err = h.Deployer.Readiness.ApplyAndVerify(ctx, path, accepted.Plan)
		return err
	})
	if err != nil {
		var infrastructure *InfrastructureError
		if errors.As(err, &infrastructure) || ctx.Err() != nil {
			return HeadResult{}, err
		}
		var image *imagesrc.Error
		if errors.As(err, &image) {
			return held(HoldBuild, err.Error()), nil
		}
		return held(HoldRollout, err.Error()), nil
	}
	if !completeDeploymentReceipt(receipt) || !rendered.SameDeploymentSource(receipt.Source, accepted.Plan.GetSource()) {
		return held(HoldRollout, "deployment returned incomplete evidence or another source"), nil
	}
	verdict, err := steps.awaitVerdict(ctx, bounds, func() (HealthVerdict, error) {
		return h.Deployer.Verifier.VerifyCandidate(ctx, path, commit, cfg.Namespace, cfg.HealthObjective, receipt)
	})
	if err != nil {
		var pending *PendingVerificationError
		if errors.As(err, &pending) {
			result := held(HoldHealth, pending.Error())
			result.Verdict = verdict
			return result, nil
		}
		return HeadResult{Verdict: verdict}, err
	}
	if verdict.Outcome != VerificationClear {
		result := held(HoldHealth, verdict.Feedback())
		result.Verdict = verdict
		return result, nil
	}
	if err := h.Broker.RecordHeadDeployment(ctx, commit, deploymentToBroker(receipt)); err != nil {
		return HeadResult{Verdict: verdict}, fmt.Errorf("record head deployment: %w", err)
	}
	return HeadResult{Outcome: HeadDeployed, Commit: commit, Verdict: verdict}, nil
}
