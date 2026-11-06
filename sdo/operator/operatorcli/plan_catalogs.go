package operatorcli

import (
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/catalog/images"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/planfile"
)

// planCatalogs are the registries a stored deployment plan decodes against:
// the same renderer and image-source IDs the deployer applies, so the
// memory check an agent runs and the broker's publication check agree.
func planCatalogs() planfile.Catalogs {
	return planfile.Catalogs{Renderers: renderers.Catalog(), Images: images.StaticCatalog()}
}

// acceptedDeploymentSource is the deployment source declared by the plan of
// the accepted checkout at root. A checkout without a plan declares none.
func acceptedDeploymentSource(root string) (*contracts.DeploymentSource, error) {
	plan, err := planfile.Read(root, planCatalogs())
	if errors.Is(err, planfile.ErrAbsent) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return plan.GetSource(), nil
}
