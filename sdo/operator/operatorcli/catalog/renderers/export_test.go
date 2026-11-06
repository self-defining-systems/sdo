package renderers

import (
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
)

// CatalogWithChildRunner is the production helm wiring over a replacement for
// the sandbox's child runner, so a test can produce spawn failures.
func CatalogWithChildRunner(run childRunner) *render.Catalog {
	c := render.NewCatalog()
	if err := helm.Register(c, isolatedHelmWith(run)); err != nil {
		panic(err)
	}
	return c
}
