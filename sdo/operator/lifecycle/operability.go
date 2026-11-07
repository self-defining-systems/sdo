package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
)

// OfflineChecker computes the operability checks that need no cluster for a
// candidate checkout and plan. The trusted Deployer implements it with the same
// effective tree, render environment and shared functions it applies with, so
// an error found here is the error apply would have hit, before any cluster
// effect. A Readiness verifier that does not implement it skips the gate.
type OfflineChecker interface {
	CheckOffline(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (OfflineChecks, error)
}

// OfflineChecks are the checks plus the tree and digest identities they were
// computed from (empty when a failure kept them from being computed).
type OfflineChecks struct {
	Checks                                                          []operability.Check
	BaseTree, EffectiveTree, PatchesDigest, RenderDigest, EnvDigest string
}

// OperabilityError is a terminal outcome of the offline gate or the candidate gate: no agent can correct it
// (the cause belongs to a person, to SDO itself, or the plan was supplied by a
// person). It carries the report and the decision recomputed from it.
type OperabilityError struct {
	Report   operability.Report
	Decision operability.Decision
}

func (e *OperabilityError) Error() string {
	return fmt.Sprintf("operability checks failed (owner %s):\n%s", e.Decision.Owner, e.Decision.Feedback())
}

// retryOffline marks an infrastructure cause found by the offline checks so the
// coordinator's bounded retry applies and no deployer attempt is spent.
type retryOffline struct{ decision operability.Decision }

func (e *retryOffline) Error() string {
	return "offline operability checks hit an infrastructure failure:\n" + e.decision.Feedback()
}

func (e *retryOffline) InfrastructureOutcome() bool { return true }

// offlineGate runs the offline report for the candidate before any cluster
// effect. ok is false when the Readiness verifier cannot check offline. The
// returned decision is Accepted, or says who must act; infrastructure causes are
// retried within the bounded window and end as an *InfrastructureError.
func (c Coordinator) offlineGate(ctx context.Context, cfg Config, proposal broker.Proposal, plan *contracts.DeploymentPlan) (report operability.Report, decision operability.Decision, ok bool, err error) {
	checker, ok := c.Readiness.(OfflineChecker)
	if !ok {
		return operability.Report{}, operability.Decision{}, false, nil
	}
	mode := operability.ModeFresh
	if cfg.ProvidedPlan != nil {
		mode = operability.ModeProvided
	}
	err = c.retryInfrastructure(ctx, cfg, "offline operability checks", func() error {
		checks, err := checker.CheckOffline(ctx, proposal.Path, plan)
		if err != nil {
			return err
		}
		subject := operability.Subject{
			AcceptedCommit: proposal.BaseCommit, BaseTree: checks.BaseTree, EffectiveTree: checks.EffectiveTree,
			PatchesDigest: checks.PatchesDigest, RenderDigest: checks.RenderDigest, EnvDigest: checks.EnvDigest,
		}
		report, err = operability.New(operability.StageDeployment, subject.WithUnavailable()).With(checks.Checks...)
		if err != nil {
			return err
		}
		decision = report.DecideIn(mode, operability.StageItems(operability.StageDeployment)...)
		if decision.Kind == operability.Retry {
			return &retryOffline{decision}
		}
		return nil
	})
	return report, decision, true, err
}

// offlineOutcome turns a non-accepted decision into what deploy returns: a
// correction for the deployer when that is the owner, otherwise a terminal
// *OperabilityError.
func offlineOutcome(report operability.Report, decision operability.Decision) (feedback string, err error) {
	if decision.Kind == operability.Correct && decision.Owner == operability.OwnerDeployer {
		return "the deployment failed the offline operability checks (no cluster was touched):\n" + decision.Feedback(), nil
	}
	return "", &OperabilityError{Report: report, Decision: decision}
}

// asOperabilityError reports whether err is a terminal offline-gate outcome.
func asOperabilityError(err error) (*OperabilityError, bool) {
	var opErr *OperabilityError
	return opErr, errors.As(err, &opErr)
}

// deploymentReport is the stage-deployment report of an applied deployment: the
// offline checks the gate ran (none when the verifier cannot check offline)
// and the live checks of the receipt, bound to the receipt's identity and to
// the commit the proposal was based on. The broker recomputes the binding and
// the decision from it at PublishVerifiedSource.
func (c Coordinator) deploymentReport(ctx context.Context, cfg Config, proposal broker.Proposal, plan *contracts.DeploymentPlan, offline operability.Report, receipt DeploymentReceipt) (report operability.Report, decision operability.Decision, err error) {
	mode := c.mode(cfg)
	err = c.retryInfrastructure(ctx, cfg, "live operability checks", func() error {
		live, err := c.Readiness.CheckLive(ctx, proposal.Path, plan, receipt, nil)
		if err != nil {
			return err
		}
		var checks []operability.Check
		for _, item := range offline.Items {
			checks = append(checks, item.Checks...)
		}
		subject := operability.Subject{
			AcceptedCommit: proposal.BaseCommit, BaseTree: receipt.BaseTree, EffectiveTree: receipt.EffectiveTree,
			PatchesDigest: receipt.PatchesDigest, RenderDigest: receipt.RenderDigest, EnvDigest: receipt.EnvDigest,
		}
		if report, err = operability.New(operability.StageDeployment, subject).With(operability.Combine(checks, live)...); err != nil {
			return err
		}
		decision = report.DecideIn(mode, operability.StageItems(operability.StageDeployment)...)
		if decision.Kind == operability.Retry {
			return &retryOffline{decision}
		}
		return nil
	})
	return report, decision, err
}

// deployedOutcome turns a non-accepted decision about an applied deployment
// into a correction for the deployer or a terminal *OperabilityError.
func deployedOutcome(report operability.Report, decision operability.Decision) (feedback string, err error) {
	if decision.Kind == operability.Correct && decision.Owner == operability.OwnerDeployer {
		return "the deployment was applied but failed the operability checks:\n" + decision.Feedback(), nil
	}
	return "", &OperabilityError{Report: report, Decision: decision}
}

// operabilityPublisher is an installer that exposes the verified report of the
// accepted health commit (the sdo-operability ConfigMap).
type operabilityPublisher interface {
	PublishOperability(ctx context.Context, namespace string, report operability.Report) error
}

// exposeOperability reads the accepted health commit's report back from the
// broker, which verifies it again, and hands it to an installer that can expose
// it. It runs before the controller is installed, so the summary exists when
// the lane reports installed. A missing report is an error, never a silent skip.
func (c Coordinator) exposeOperability(ctx context.Context, judgeID string, cfg Config) error {
	publisher, ok := c.Installer.(operabilityPublisher)
	if !ok {
		return nil
	}
	report, found, err := c.Broker.OperabilityReport(ctx, judgeID, operability.StageComplete)
	if err != nil {
		return fmt.Errorf("accepted health commit's operability report: %w", err)
	}
	if !found {
		return fmt.Errorf("accepted health commit %s has no operability report", judgeID)
	}
	if err := publisher.PublishOperability(ctx, cfg.Namespace, report); err != nil {
		return fmt.Errorf("expose operability summary: %w", err)
	}
	return nil
}
