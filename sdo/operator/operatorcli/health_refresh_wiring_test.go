package operatorcli

import (
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

// The controller's health refresh verifies the judge's candidate against the
// accepted deployment, so its Coordinator carries a deployer and a verifier.
// Scenario 1 of the controller end-to-end run reached the refresh with neither
// and panicked; this is the cheap check that the wiring has them.
func TestHealthRefreshCoordinatorCarriesDeployerAndVerifier(t *testing.T) {
	launch := &contracts.ControllerConfig{
		Namespace: "app", AppRoot: "/repo", BrokerWorktreeRoot: "/mnt/repo/workspaces", RepositoryMountPath: "/mnt/repo",
	}
	coordinator, err := healthRefreshCoordinator(launch, "/opt/sdo", fake.NewSimpleClientset(), nil, &worker_registry.Registry{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if coordinator.Readiness == nil || coordinator.Verifier == nil {
		t.Fatalf("coordinator = %+v, want a deployer and a verifier", coordinator)
	}
}
