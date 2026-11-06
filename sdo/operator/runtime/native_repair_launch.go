package runtime

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/deploywire"
)

// repairsFromLaunch is the controller child's SourceRepairer: the trusted
// Deployer of the identity in the typed launch config (absent means nothing
// can build, so only a repair that moves no image deploys), around the
// child's own broker.
//
// Candidate verification needs the Go detector runner: with shell detectors
// it evaluates only the accepted checkout, so no repairer is configured and a
// repair is published without a deployment, as before.
func repairsFromLaunch(config *contracts.ControllerConfig, client kubernetes.Interface, repository *broker.Broker) (SourceRepairer, error) {
	if source := config.GetCodePath().GetOverrides().GetDetectorSource(); config.GetFrozenMemory() || source == "shell" || source == "" && config.GetCodePath().GetName() == "shell" {
		return nil, nil
	}
	identity, err := deploywire.IdentityOf(config)
	if err != nil {
		return nil, err
	}
	deployer, err := deploywire.RepairDeployer(identity, config, builder.DefaultOperatorRoot(), client, repository)
	if err != nil {
		return nil, fmt.Errorf("source repair deployer: %w", err)
	}
	return DeploymentRepairer{Deployer: deployer, Namespace: config.GetNamespace(), AppRoot: config.GetAppRoot()}, nil
}
