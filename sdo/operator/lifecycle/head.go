package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

// HeadDeployments is what the holds may also do: keep a notice of every head that was deployed. A deployment adds no
// commit and no trailer to history (the head is someone else's commit), so whoever pushed it learns that SDO deployed
// it, and which verdict the verifier gave, from this notice.
type HeadDeployments interface {
	Deployed(ctx context.Context, change broker.HeadChange, result HeadResult) error
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
	if notices, ok := h.Holds.(HeadDeployments); ok && out.Outcome == HeadDeployed {
		if err := notices.Deployed(ctx, change, out); err != nil {
			return out, fmt.Errorf("record head deployment notice: %w", err)
		}
	}
	return out, nil
}

func (h HeadDeployer) deploy(ctx context.Context, cfg RepairConfig, commit string) (HeadResult, error) {
	accepted, _, err := h.Broker.AcceptedSourceEvidence(ctx)
	if err != nil && !errors.Is(err, planfile.ErrAbsent) {
		return HeadResult{}, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if err != nil || accepted.Plan.GetSource() == nil {
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
	// The verdict's own preparation overlaps the rollout when the verifier can: the detectors compile
	// while the pods roll, and the prober's required samples accumulate from the moment the deployment
	// is applied, so the verdict after the rollout reads them instead of waiting for them.
	var session CandidateSession
	applyCtx := ctx
	// A session needs the clock: a verdict only counts evaluations judged after the rollout.
	if sv, ok := h.Deployer.Verifier.(SessionVerifier); ok && h.Deployer.Now != nil {
		begun, err := sv.BeginCandidate(ctx, path, commit, cfg.Namespace, cfg.HealthObjective)
		if err != nil && !errors.Is(err, ErrNoSession) {
			return HeadResult{}, err
		}
		if err == nil {
			session = begun
			defer session.Close()
			applyCtx = WithAppliedHook(ctx, func(pins rendered.ReceiptPins) {
				if err := session.Applied(ctx, pins); err != nil {
					slog.Warn("head_verification_session_not_started", "commit", commit, "error", err)
				}
			})
		}
	}
	var receipt DeploymentReceipt
	err = steps.retryInfrastructure(ctx, bounds, "head deployment", func() (err error) {
		receipt, err = h.Deployer.Readiness.ApplyAndVerify(applyCtx, path, accepted.Plan)
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
	var readyAt time.Time
	if h.Deployer.Now != nil {
		readyAt = h.Deployer.Now()
	}
	evaluate := func() (HealthVerdict, error) {
		if session != nil {
			return session.Verdict(ctx, receipt, readyAt)
		}
		return h.Deployer.Verifier.VerifyCandidate(ctx, path, commit, cfg.Namespace, cfg.HealthObjective, receipt)
	}
	verdict, err := steps.awaitVerdict(ctx, bounds, evaluate)
	if err == nil && verdict.Outcome == VerificationSourceFault {
		// A rollout disturbs its application for a while (workers restart and
		// retry what failed meanwhile), so detectors that fire right after it
		// are not yet the release's fault: the same release was accepted in one
		// run and held in the next, by whether the one evaluation fell in that
		// minute. The pending window is waited out for the fault to clear; only
		// one still there when it ends holds the head, with that last verdict.
		verdict, err = steps.awaitClear(ctx, bounds, verdict, evaluate)
	}
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
