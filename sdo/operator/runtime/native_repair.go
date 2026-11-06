package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// SourceRepair is what accepting a responder's source repair concluded.
type SourceRepair struct {
	// Commit is the accepted proposal commit; empty when nothing was accepted.
	Commit string
	// Held names why a repair that changes deployment inputs was not accepted
	// (a typed hold, "rollout: ..."). The cluster is left as it is.
	Held string
	// Deployed reports that the trusted Deployer applied and verified the
	// repair's effective tree before it was accepted.
	Deployed bool
	// Undeployed reports an accepted repair no SDO deployment could apply (an
	// installation with no recorded deployment).
	Undeployed bool
}

// SourceRepairer accepts the source repair of an incident's responder
// proposal. An accepted change to deployment inputs (patches, values,
// manifests, build, overlays) is deployed and verified by the trusted
// Deployer before the broker accepts it, so the effective tree that is
// accepted is the one that runs.
type SourceRepairer interface {
	Repair(ctx context.Context, incidentID string) (SourceRepair, error)
}

// DeploymentRepairer is SourceRepairer over lifecycle.RepairDeployer: the
// same Deployer and independent verification an upstream advance uses.
type DeploymentRepairer struct {
	Deployer  lifecycle.RepairDeployer
	Namespace string
	// AppRoot is the accepted repository; its goal holds the health
	// objective the candidate's detectors are judged against.
	AppRoot string
}

// Repair implements SourceRepairer.
func (r DeploymentRepairer) Repair(ctx context.Context, incidentID string) (SourceRepair, error) {
	objective, err := memory.AcceptedHealthObjective(r.AppRoot)
	if err != nil {
		return SourceRepair{}, err
	}
	result, err := r.Deployer.Repair(ctx, lifecycle.RepairConfig{Namespace: r.Namespace, HealthObjective: objective}, incidentID)
	if err != nil {
		return SourceRepair{}, err
	}
	switch result.Outcome {
	case lifecycle.RepairNone:
		return SourceRepair{}, nil
	case lifecycle.RepairPublished:
		return SourceRepair{Commit: result.Receipt.Commit}, nil
	case lifecycle.RepairUndeployed:
		return SourceRepair{Commit: result.Receipt.Commit, Undeployed: true}, nil
	case lifecycle.RepairDeployed:
		return SourceRepair{Commit: result.Receipt.Commit, Deployed: true}, nil
	case lifecycle.RepairHeld:
		return SourceRepair{Held: fmt.Sprintf("%s: %s", result.Hold, result.Detail)}, nil
	}
	return SourceRepair{}, fmt.Errorf("unknown source repair outcome %q", result.Outcome)
}

// ErrRepairerRequired: a responder repair changes deployment inputs but this
// configuration declares no repairer. Only a configuration that cannot deploy
// a candidate (PlainPublication) may accept such a repair without one;
// anything else would publish a tree that never ran.
var ErrRepairerRequired = errors.New("source repair changes deployment inputs but no repairer is configured")

// PlainPublicationReason names why a configuration has no deploying repairer.
type PlainPublicationReason string

const (
	// PublishesUndeployedShellDetectors: shell detectors only evaluate the
	// accepted checkout, so a candidate cannot be verified.
	PublishesUndeployedShellDetectors PlainPublicationReason = "shell_detectors"
	// PublishesUndeployedFrozenMemory: frozen memory deploys nothing.
	PublishesUndeployedFrozenMemory PlainPublicationReason = "frozen_memory"
)

// PlainPublicationBroker is the broker surface PlainPublication uses.
type PlainPublicationBroker interface {
	ChangedPaths(ctx context.Context, incidentID string, phase broker.Phase) ([]string, error)
	Publish(ctx context.Context, incidentID string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error)
}

// PlainPublication is the SourceRepairer of a configuration that deliberately
// has no deploying repairer: it publishes the proposal through the broker and
// reports a change to deployment inputs as Undeployed.
type PlainPublication struct {
	Repository PlainPublicationBroker
	Reason     PlainPublicationReason
}

// Repair implements SourceRepairer.
func (p PlainPublication) Repair(ctx context.Context, incidentID string) (SourceRepair, error) {
	if p.Reason == "" || p.Repository == nil {
		return SourceRepair{}, errors.New("plain source repair publication needs a repository and a declared reason")
	}
	paths, err := p.Repository.ChangedPaths(ctx, incidentID, broker.ProposalPhase)
	if err != nil {
		return SourceRepair{}, err
	}
	accepted, err := p.Repository.Publish(ctx, incidentID, broker.Responder, broker.ProposalPhase)
	return SourceRepair{Commit: accepted.Commit, Undeployed: slices.ContainsFunc(paths, layout.IsEffectiveDeploymentInput)}, err
}
