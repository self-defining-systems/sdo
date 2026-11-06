package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

// RepairBroker is the part of the broker a responder's source repair needs:
// the proposal's changed paths, plain publication for a repair that changes no
// deployment input, and the stage, deployed accept and discard of one that does.
type RepairBroker interface {
	ChangedPaths(ctx context.Context, incidentID string, phase broker.Phase) ([]string, error)
	Publish(ctx context.Context, incidentID string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error)
	// AcceptedSourceEvidence is the deployment the accepted HEAD descends
	// from, with the plan its tree declares.
	AcceptedSourceEvidence(ctx context.Context) (accepted broker.AcceptedSource, found bool, err error)
	StageCandidate(ctx context.Context, incidentID string, actor broker.Actor, phase broker.Phase) (broker.Candidate, error)
	AcceptDeployed(ctx context.Context, candidate broker.Candidate, evidence broker.SourceEvidence) (broker.Receipt, error)
	DiscardCandidate(incidentID string, phase broker.Phase) error
	// FinalizeAcceptedDeployment completes the deployment evidence of a
	// repair a redelivery finds already accepted (AcceptDeployed moved the
	// head but did not finish recording), so pins and the next advance follow
	// it. It reports whether the accepted commit is a recorded deployment.
	FinalizeAcceptedDeployment(ctx context.Context, incidentID string) (bool, error)
}

// RepairOutcome is what Repair did with a responder's proposal.
type RepairOutcome string

const (
	// RepairNone: the proposal carries no source repair.
	RepairNone RepairOutcome = "none"
	// RepairPublished: the repair changes no deployment input, so there is
	// nothing to deploy; the broker accepted it.
	RepairPublished RepairOutcome = "published"
	// RepairUndeployed: the repair changes deployment inputs but no SDO
	// deployment is recorded for this installation (a seeded install), so
	// nothing SDO owns can redeploy it. The broker accepted it without a
	// deployment; the cluster is whatever the responder left.
	RepairUndeployed RepairOutcome = "published_undeployed"
	// RepairDeployed: the trusted Deployer rendered the candidate's effective
	// tree, applied it, verified rollout and health, and the broker accepted
	// the repair with that deployment evidence (new pins).
	RepairDeployed RepairOutcome = "deployed"
	// RepairHeld: the candidate could not be deployed and verified (Hold and
	// Detail say why). Nothing is accepted; the cluster is left as it is.
	RepairHeld RepairOutcome = "held"
)

// RepairResult is the typed result of one Repair.
type RepairResult struct {
	Outcome RepairOutcome
	Hold    HoldReason
	Detail  string
	Receipt broker.Receipt
	Verdict HealthVerdict
	// Deployment is the verified receipt of a RepairDeployed outcome.
	Deployment DeploymentReceipt
}

// RepairConfig is the installation a repair deploys into and the waits.
type RepairConfig struct {
	Namespace, HealthObjective string
	PendingEvaluations         int
	PendingInterval            time.Duration
	InfrastructureTries        int
	InfrastructureBackoff      time.Duration
}

// RepairDeployer makes an accepted responder source repair reach the cluster
// through the same trusted Deployer and independent verification the upstream
// advance uses: the staged candidate's effective tree is rendered, changed
// images are built, the workloads are applied and observed, the candidate's
// detectors judge the live result, and only then does the broker accept the
// repair with the deployment evidence. A responder never applies source-owned
// objects itself.
type RepairDeployer struct {
	Broker    RepairBroker
	Readiness SourceReadinessVerifier
	Verifier  CandidateVerifier
	// Wait pauses between bounded re-evaluations and infrastructure retries;
	// nil waits on a timer.
	Wait func(context.Context, time.Duration) error
}

func (r RepairDeployer) validate(cfg RepairConfig) error {
	if r.Broker == nil || r.Readiness == nil || r.Verifier == nil {
		return errors.New("source repair needs a broker, readiness and a verifier")
	}
	if cfg.Namespace == "" || cfg.HealthObjective == "" {
		return errors.New("source repair needs a namespace and a health objective")
	}
	return nil
}

// Repair publishes or deploys the source repair in incidentID's responder
// proposal. A returned error is a rejection of the proposal by the broker or a
// transient or unexpected failure (the caller retries); a candidate the
// cluster or the detectors refuse is a RepairHeld result.
func (r RepairDeployer) Repair(ctx context.Context, cfg RepairConfig, incidentID string) (RepairResult, error) {
	if err := r.validate(cfg); err != nil {
		return RepairResult{}, err
	}
	paths, err := r.Broker.ChangedPaths(ctx, incidentID, broker.ProposalPhase)
	if err != nil {
		return RepairResult{}, err
	}
	paths = broker.SourceRepairPaths(paths)
	if len(paths) == 0 {
		return RepairResult{Outcome: RepairNone}, nil
	}
	if !changesDeploymentInputs(paths) {
		receipt, err := r.Broker.Publish(ctx, incidentID, broker.Responder, broker.ProposalPhase)
		return RepairResult{Outcome: RepairPublished, Receipt: receipt}, err
	}
	accepted, found, err := r.Broker.AcceptedSourceEvidence(ctx)
	if err != nil && !errors.Is(err, planfile.ErrAbsent) {
		return RepairResult{}, fmt.Errorf("accepted deployment evidence: %w", err)
	}
	if err != nil || !found || accepted.Plan.GetSource() == nil {
		receipt, err := r.Broker.Publish(ctx, incidentID, broker.Responder, broker.ProposalPhase)
		return RepairResult{Outcome: RepairUndeployed, Receipt: receipt, Detail: ErrNoAcceptedPlan.Error()}, err
	}
	for restage := 0; ; restage++ {
		result, err := r.deploy(ctx, cfg, incidentID, accepted.Plan)
		if !errors.Is(err, broker.ErrCandidateStale) {
			return result, err
		}
		if restage >= maxCandidateRestages {
			return result, fmt.Errorf("the accepted head moved %d times while the repair was deployed: %w", restage+1, err)
		}
	}
}

func changesDeploymentInputs(paths []string) bool {
	for _, path := range paths {
		if layout.IsEffectiveDeploymentInput(path) {
			return true
		}
	}
	return false
}

// deploy stages the proposal on the current head, deploys and verifies the
// staged tree and accepts it with the deployment evidence. A stale stage
// (ErrCandidateStale) is returned for Repair to restage.
func (r RepairDeployer) deploy(ctx context.Context, cfg RepairConfig, incidentID string, plan *contracts.DeploymentPlan) (RepairResult, error) {
	candidate, err := r.Broker.StageCandidate(ctx, incidentID, broker.Responder, broker.ProposalPhase)
	if err != nil {
		return RepairResult{}, err
	}
	if candidate.Commit == "" {
		return RepairResult{Outcome: RepairNone}, nil
	}
	if candidate.Accepted {
		// Accepted is not deployed evidence: finish what a crash between the
		// head move and the evidence finalization left pending.
		if _, err := r.Broker.FinalizeAcceptedDeployment(ctx, incidentID); err != nil {
			return RepairResult{}, fmt.Errorf("finalize accepted source repair evidence: %w", err)
		}
		return RepairResult{Outcome: RepairDeployed, Receipt: broker.Receipt{IncidentID: incidentID, Phase: broker.ProposalPhase, Commit: candidate.Commit}}, nil
	}
	accepted := false
	defer func() {
		if !accepted {
			_ = r.Broker.DiscardCandidate(incidentID, broker.ProposalPhase)
		}
	}()
	steps := Coordinator{Wait: r.Wait}
	bounds := Config{Namespace: cfg.Namespace, HealthObjective: cfg.HealthObjective, PendingEvaluations: cfg.PendingEvaluations, PendingInterval: cfg.PendingInterval, InfrastructureTries: cfg.InfrastructureTries, InfrastructureBackoff: cfg.InfrastructureBackoff}
	var receipt DeploymentReceipt
	err = steps.retryInfrastructure(ctx, bounds, "source repair deployment", func() (err error) {
		receipt, err = r.Readiness.ApplyAndVerify(ctx, candidate.Path, plan)
		return err
	})
	if err != nil {
		var infrastructure *InfrastructureError
		if errors.As(err, &infrastructure) || ctx.Err() != nil {
			return RepairResult{}, err
		}
		var image *imagesrc.Error
		if errors.As(err, &image) {
			return repairHeld(HoldBuild, err.Error()), nil
		}
		return repairHeld(HoldRollout, err.Error()), nil
	}
	if !completeDeploymentReceipt(receipt) || !rendered.SameDeploymentSource(receipt.Source, plan.GetSource()) {
		return repairHeld(HoldRollout, "deployment returned incomplete evidence or another source"), nil
	}
	verdict, err := steps.awaitVerdict(ctx, bounds, func() (HealthVerdict, error) {
		return r.Verifier.VerifyCandidate(ctx, candidate.Path, candidate.Commit, cfg.Namespace, cfg.HealthObjective, receipt)
	})
	if err != nil {
		var pending *PendingVerificationError
		if errors.As(err, &pending) {
			held := repairHeld(HoldHealth, pending.Error())
			held.Verdict = verdict
			return held, nil
		}
		return RepairResult{Verdict: verdict}, err
	}
	if verdict.Outcome != VerificationClear {
		held := repairHeld(HoldHealth, verdict.Feedback())
		held.Verdict = verdict
		return held, nil
	}
	receiptOfRepair, err := r.Broker.AcceptDeployed(ctx, candidate, deploymentToBroker(receipt))
	if err != nil {
		return RepairResult{Verdict: verdict}, fmt.Errorf("accept deployed source repair: %w", err)
	}
	accepted = true
	return RepairResult{Outcome: RepairDeployed, Receipt: receiptOfRepair, Verdict: verdict, Deployment: receipt}, nil
}

func repairHeld(reason HoldReason, detail string) RepairResult {
	return RepairResult{Outcome: RepairHeld, Hold: reason, Detail: detail}
}
