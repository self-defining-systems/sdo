package deploywire

import (
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
)

// Verifier is the independent health verifier of a controller launch: it
// judges a candidate checkout's detectors against the live deployment that
// readiness applied.
func Verifier(launch *contracts.ControllerConfig, operatorRoot string, readiness *deployment.Deployer) deployment.IndependentHealthVerifier {
	return deployment.IndependentHealthVerifier{
		AcceptedRoot: launch.GetAppRoot(), OperatorRoot: operatorRoot, Readiness: readiness,
		Probe: deployment.AcceptedDetectorProbe{Replay: launch.GetReplay(), MetricsURL: launch.GetMetricsUrl()},
	}
}

// RepairDeployer is the controller's deployment of a responder's accepted
// source repair: the trusted Deployer of identity and the independent
// verifier, around the controller's own broker.
func RepairDeployer(identity Identity, launch *contracts.ControllerConfig, operatorRoot string, client kubernetes.Interface, repository lifecycle.RepairBroker) (lifecycle.RepairDeployer, error) {
	deployer, err := identity.Deployer(client, TargetOf(launch), launch.GetRenderEnvironment())
	if err != nil {
		return lifecycle.RepairDeployer{}, err
	}
	return lifecycle.RepairDeployer{Broker: repository, Readiness: deployer, Verifier: Verifier(launch, operatorRoot, &deployer)}, nil
}

// The controller's broker is the repair's broker.
var _ lifecycle.RepairBroker = (*broker.Broker)(nil)
