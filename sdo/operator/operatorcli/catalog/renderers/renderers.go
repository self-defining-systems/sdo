// Package renderers is the single wiring point for deployment renderers. The
// generated controller main, sdo-shell-controller and the operator CLI import it;
// no other package names a renderer implementation.
package renderers

import (
	"fmt"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/raw"
)

// Catalog returns a catalog holding every built-in renderer: raw,
// kustomize and helm. Registration of a built-in cannot fail, so a failure is a
// programming error and panics.
func Catalog() *render.Catalog {
	c := render.NewCatalog()
	for _, register := range []func(*render.Catalog) error{raw.Register, kustomize.Register, helm.Register} {
		if err := register(c); err != nil {
			panic(fmt.Sprintf("renderers: register built-in renderer: %v", err))
		}
	}
	return c
}
