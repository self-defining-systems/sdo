package rendered

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// OverrideHint names the file that settles what the files cannot say.
const OverrideHint = ".sdo/deployment/plan.yaml"

// ResolveSource is the deployment source of a tree: the override when the
// repository names one, otherwise the one its files imply (DeriveSource).
// Every reader that renders a tree resolves its source here, so deploy, drift,
// restore and shape read the same thing.
func ResolveSource(fsys render.FS, cat *render.Catalog, override *contracts.DeploymentSource) (*contracts.DeploymentSource, error) {
	if override != nil {
		return override, nil
	}
	return DeriveSource(fsys, cat)
}

// DeriveSource reads the source from the files present: each renderer of the
// catalog reports the roots it recognizes (render.Catalog.Detect). A root
// inside another root belongs to it. One root is the source; several are
// ambiguous, and a person or the deployer names one in the override. With no
// root, a fallback renderer's root (raw: the files that are all Kubernetes
// objects) is the source; with none, the tree has no deployment files
// (SourceUnconfigured).
func DeriveSource(fsys render.FS, cat *render.Catalog) (*contracts.DeploymentSource, error) {
	if cat == nil {
		return nil, &SourceError{Kind: SourceUnconfigured, Err: errors.New("no deployment source is configured and no renderer catalog is wired to derive one")}
	}
	detected, err := cat.Detect(fsys)
	if err != nil {
		return nil, &SourceError{Kind: SourceInvalid, Err: err}
	}
	var roots, fallback []render.Detected
	for _, d := range detected {
		if d.Fallback {
			fallback = append(fallback, d)
		} else {
			roots = append(roots, d)
		}
	}
	roots = slices.DeleteFunc(slices.Clone(roots), func(d render.Detected) bool {
		return slices.ContainsFunc(roots, func(other render.Detected) bool { return render.Within(d.Dir, other.Dir) })
	})
	if len(roots) == 0 {
		roots = fallback
	}
	switch len(roots) {
	case 0:
		return nil, &SourceError{Kind: SourceUnconfigured, Err: errors.New("the tree has no deployment files: no Helm chart (Chart.yaml), no kustomization.yaml and no Kubernetes manifest")}
	case 1:
		return &contracts.DeploymentSource{RendererId: roots[0].RendererID, Config: roots[0].Config}, nil
	}
	names := make([]string, 0, len(roots))
	for _, d := range roots {
		dir := d.Dir
		if dir == "." {
			dir = "the repository root"
		}
		names = append(names, fmt.Sprintf("%s (%s)", dir, d.RendererID))
	}
	return nil, &SourceError{Kind: SourceInvalid, Err: fmt.Errorf("the tree has several deployment roots: %s; name the one to deploy in %s", strings.Join(names, ", "), OverrideHint)}
}
