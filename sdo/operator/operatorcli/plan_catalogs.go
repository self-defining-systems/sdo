package operatorcli

import (
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
