package operatorcli

import (
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/operatorcli/deploywire"
)

// deployIdentity is the trusted in-cluster build and apply identity (see
// deploywire.Identity). The operate Job deploys onboarding with it, the
// controller parent deploys upstream advances with it and the controller
// child deploys a responder's source repair with it.
type deployIdentity deploywire.Identity

func (d deployIdentity) validate() error { return deploywire.Identity(d).Validate() }

// Enabled reports whether the identity can build.
func (d deployIdentity) Enabled() bool { return deploywire.Identity(d).Enabled() }

// deployTarget is where a deployIdentity deploys.
type deployTarget deploywire.Target

// deployer is the trusted Deployer of the identity (deploywire.Identity.Deployer).
func (d deployIdentity) deployer(client kubernetes.Interface, target deployTarget, environment *contracts.RenderEnvironment, policy *contracts.DeploymentPolicy) (deployment.Deployer, error) {
	return deploywire.Identity(d).Deployer(client, deploywire.Target(target), environment, policy)
}

func controlNamespace(launch *contracts.ControllerConfig) string {
	return deploywire.ControlNamespace(launch)
}
