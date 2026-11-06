package runtime

import (
	"errors"
	"regexp"

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
