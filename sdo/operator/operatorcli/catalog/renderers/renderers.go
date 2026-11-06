// Package renderers is the single wiring point for deployment renderers. The
// generated controller main, sdo-shell-controller and the operator CLI import it;
// no other package names a renderer implementation.
package renderers

import (
	"context"
	"errors"
	"fmt"

	"sdo.dev/operator/internal/sandboxio"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/raw"
)

// helmChild names the sandboxed Helm render. Every binary that links this
// package serves it, so the child is always the caller's own executable.
const helmChild = "helm-render"

func init() { sandboxio.Register(helmChild, helm.Serve) }

// isolatedHelm executes chart templates, which are Git content an agent can
// edit, in a child process bounded in memory, time and output: a template that
// exhausts memory or loops without end fails the render, never the caller.
func isolatedHelm(ctx context.Context, request []byte) ([]byte, error) {
	return isolatedHelmWith(sandboxio.Run)(ctx, request)
}

// childRunner is sandboxio.Run's shape.
type childRunner func(ctx context.Context, name string, request []byte, limits sandboxio.Limits) ([]byte, error)

// isolatedHelmWith classifies what run reports. A child that ran and broke a
// limit or crashed is a verdict on the chart (a render-budget violation). A
// child that could not be started (fork or exec refused under host resource
// pressure) says nothing about the chart: it is render.ErrTransient, which a
// deployment source classifies as unavailable, never invalid.
func isolatedHelmWith(run childRunner) helm.Runner {
	return func(ctx context.Context, request []byte) ([]byte, error) {
		out, err := run(ctx, helmChild, request, sandboxio.DefaultLimits)
		switch {
		case errors.Is(err, sandboxio.ErrChild):
			return nil, fmt.Errorf("%w: %w", helm.ErrRenderProcess, err)
		case errors.Is(err, sandboxio.ErrSpawn):
			return nil, fmt.Errorf("%w: %w", render.ErrTransient, err)
		}
		return out, err
	}
}

// Catalog returns a catalog holding every built-in renderer: raw,
// kustomize and helm (isolated). Registration of a built-in cannot fail, so a
// failure is a programming error and panics.
func Catalog() *render.Catalog {
	c := render.NewCatalog()
	registerHelm := func(c *render.Catalog) error { return helm.Register(c, isolatedHelm) }
	for _, register := range []func(*render.Catalog) error{raw.Register, kustomize.Register, registerHelm} {
		if err := register(c); err != nil {
			panic(fmt.Sprintf("renderers: register built-in renderer: %v", err))
		}
	}
	return c
}
