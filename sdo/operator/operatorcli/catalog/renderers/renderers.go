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

// Subset returns a catalog holding only the named built-ins. The operate path
// uses it to offer the agent exactly the renderers its deployer can deploy.
func Subset(ids ...string) (*render.Catalog, error) {
	registers := map[string]func(*render.Catalog) error{raw.ID: raw.Register, kustomize.ID: kustomize.Register, helm.ID: helm.Register}
	c := render.NewCatalog()
	for _, id := range ids {
		register, ok := registers[id]
		if !ok {
			return nil, fmt.Errorf("renderers: no built-in renderer %q", id)
		}
		if err := register(c); err != nil {
			return nil, fmt.Errorf("renderers: register %s: %w", id, err)
		}
	}
	return c, nil
}
