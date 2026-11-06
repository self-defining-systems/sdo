package runtime

import (
	"errors"
	"regexp"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/rendered"
)

var gitRevision = regexp.MustCompile(`^[a-fA-F0-9]{40,64}$`)

// GitPlanSource renders accepted source as it is written; raw revision reads are
// retained for explicit historical source inspection.
type GitPlanSource = rendered.Source

var ErrAcceptedManifestAbsent = rendered.ErrAcceptedManifestAbsent

// acceptedDeploymentSource is the deployment source the accepted checkout at
// root declares in .sdo/deployment/plan.yaml. A checkout without a plan has no
// source (reads fail with SourceUnconfigured); the operator-CLI controller,
// which launches this process, refuses to start without one. The structural
// decode runs here; renderer configs are decoded again when the source renders.
func acceptedDeploymentSource(root string) (*contracts.DeploymentSource, error) {
	plan, err := planfile.Read(root, planfile.Catalogs{})
	if errors.Is(err, planfile.ErrAbsent) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return plan.GetSource(), nil
}

// acceptedImagePins is the pin provider of a native controller: its broker's
// evidence for the newest accepted deployment. Without a native broker (tests,
// embedded runs with no ledger) there are no pins and desired state keeps the
// source image.
func acceptedImagePins(native *NativeIncidentBroker) rendered.PinProvider {
	if native == nil || native.Repository == nil {
		return nil
	}
	return native.Repository
}

var _ rendered.PinProvider = (*broker.Broker)(nil)
