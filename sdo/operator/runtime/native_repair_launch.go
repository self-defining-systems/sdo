package runtime

import (
	"errors"
	"fmt"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
)

// RepairDeployerFactory builds the trusted deployment of a responder's
// accepted source repair from the typed launch config (it carries the deploy
// identity). The composition root supplies it: runtime sits below the CLI
// wiring that knows the renderer and image catalogs, so it never constructs a
// Deployer itself.
type RepairDeployerFactory func(config *contracts.ControllerConfig, client kubernetes.Interface, repository lifecycle.RepairBroker) (lifecycle.RepairDeployer, error)

// repairsFromLaunch is the controller child's SourceRepairer: the trusted
// Deployer of the identity in the typed launch config (absent means nothing
// can build, so only a repair that moves no image deploys), around the
// child's own broker.
//
// Candidate verification needs the Go detector runner: with shell detectors
// it evaluates only the accepted checkout, so the repairer is a declared
// PlainPublication: a repair is published without a deployment, as before. Every other launch
// requires the factory: a missing one fails the launch instead of silently
// publishing repairs undeployed.
func repairsFromLaunch(config *contracts.ControllerConfig, frozenMemory bool, client kubernetes.Interface, repository *broker.Broker, factory RepairDeployerFactory) (SourceRepairer, error) {
	source := config.GetCodePath().GetOverrides().GetDetectorSource()
	if frozenMemory {
		return PlainPublication{Repository: repository, Reason: PublishesUndeployedFrozenMemory}, nil
	}
	if source == "shell" || source == "" && config.GetCodePath().GetName() == "shell" {
		return PlainPublication{Repository: repository, Reason: PublishesUndeployedShellDetectors}, nil
	}
	if factory == nil {
		return nil, errors.New("source repair deployer is not wired into this controller")
	}
	deployer, err := factory(config, client, repository)
	if err != nil {
		return nil, fmt.Errorf("source repair deployer: %w", err)
	}
	return DeploymentRepairer{Deployer: deployer, Namespace: config.GetNamespace(), AppRoot: config.GetAppRoot()}, nil
}
