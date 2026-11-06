package deploywire

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
	"sdo.dev/operator/telemetryapply"
)

// TelemetryApplier is the controller's applier of designer-selected telemetry
// backends. It derives the policy from the launch's telemetry environment
// through broker.ControllerTelemetry, the derivation every controller broker
// validates under, so a backend is applied under the policy it was accepted
// under.
func TelemetryApplier(launch *contracts.ControllerConfig, client kubernetes.Interface) (lifecycle.TelemetryApplier, error) {
	derived, err := broker.ControllerTelemetry(launch, telemetrysources.Registry())
	if err != nil {
		return nil, fmt.Errorf("telemetry applier: %w", err)
	}
	return telemetryapply.NewApplier(client, derived.Policy, telemetrysources.Recipes()), nil
}

// AppliedVerifier is Verifier with the apply phase in front: for the paths
// (upstream advance, source repair) that verify accepted memory outside the
// Coordinator gate.
func AppliedVerifier(launch *contracts.ControllerConfig, operatorRoot string, readiness *deployment.Deployer, client kubernetes.Interface) (lifecycle.TelemetryAppliedVerifier, error) {
	apply, err := TelemetryApplier(launch, client)
	if err != nil {
		return lifecycle.TelemetryAppliedVerifier{}, err
	}
	return lifecycle.TelemetryAppliedVerifier{Apply: apply, Next: Verifier(launch, operatorRoot, readiness)}, nil
}
