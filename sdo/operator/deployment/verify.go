package deployment

import (
	"context"
	"errors"
	"fmt"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
)

// RendererChangedError reports that re-rendering the accepted source no longer
// yields what was deployed: the renderer (or the install environment) changed
// while the source tree did not. It is not drift of the running application.
type RendererChangedError struct {
	// What names the digest that differs: "render" or "env".
	What string
	Want string
	Got  string
}

func (e *RendererChangedError) Error() string {
	return fmt.Sprintf("accepted source renders differently than when it was deployed (%s digest %s, now %s): the renderer or its environment changed, not the running application", e.What, e.Want, e.Got)
}

func (d Deployer) git(ctx context.Context, args ...string) (string, error) {
	var (
		output []byte
		err    error
	)
	if d.Git != nil {
		output, err = d.Git(ctx, d.AcceptedRoot, args...)
	} else {
		output, err = deploymentio.Run(ctx, "git", append([]string{"-C", d.AcceptedRoot}, args...), nil)
	}
	if err != nil {
		return "", fmt.Errorf("accepted git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}

// VerifyAccepted rechecks persisted deployment evidence after a restart: the
// commit must be in the accepted branch with exactly the deployed tree, the
// accepted source must still render to the recorded digests (otherwise
// *RendererChangedError), and every recorded workload must still be ready at
// that revision with the recorded images.
func (d Deployer) VerifyAccepted(ctx context.Context, commit, namespace string, prior lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	if err := d.validate(); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if d.AcceptedRoot == "" || commit == "" || namespace == "" || !prior.Ready || prior.BaseTree == "" || len(prior.ResourceIDs) == 0 {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted repository, commit, namespace, and trusted source evidence are required")
	}
	if namespace != d.Namespace {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted source namespace differs from the deployer's namespace")
	}
	tree, err := acceptedTree(ctx, d.git, commit, prior)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if err := verifyRendered(ctx, rendered.ObjectStore{Dir: d.AcceptedRoot, Git: d.Git}, tree, d.env(render.SecretsOmit), d.Renderers, prior); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	var images []string
	for _, id := range prior.ResourceIDs {
		parts := strings.Split(id, "/")
		if len(parts) != 3 || parts[1] != d.Namespace {
			return lifecycle.DeploymentReceipt{}, fmt.Errorf("workload %q is outside the deployer's namespace", id)
		}
		observed, err := d.Applier.Observe(ctx, id)
		if err != nil {
			return lifecycle.DeploymentReceipt{}, err
		}
		if !observed.Ready || observed.ResourceID != id || observed.Revision != prior.BaseTree {
			return lifecycle.DeploymentReceipt{}, fmt.Errorf("workload %s no longer matches accepted source", id)
		}
		if len(prior.ContainerImages) > 0 {
			var expected []rendered.DeployedImage
			for _, image := range prior.ContainerImages {
				if image.Ref.Kind+"/"+image.Ref.Namespace+"/"+image.Ref.Name == id {
					expected = append(expected, image)
				}
			}
			if !sameNamedImages(expected, observed.ContainerImages) {
				return lifecycle.DeploymentReceipt{}, fmt.Errorf("workload %s named images differ from recorded deployment", id)
			}
		}
		images = append(images, observed.Images...)
	}
	if !equalStrings(images, prior.Images) {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted workload images differ from deployed images")
	}
	return prior, nil
}

// acceptedTree checks that commit is in the accepted branch and has exactly the
// tree the receipt deployed, and returns that tree.
func acceptedTree(ctx context.Context, git func(context.Context, ...string) (string, error), commit string, prior lifecycle.DeploymentReceipt) (string, error) {
	head, err := git(ctx, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	if _, err := git(ctx, "merge-base", "--is-ancestor", commit, head); err != nil {
		return "", fmt.Errorf("source commit is not in the accepted branch: %w", err)
	}
	tree, err := git(ctx, "rev-parse", commit+"^{tree}")
	if err != nil {
		return "", err
	}
	if tree != prior.BaseTree || prior.ObservedRevision != tree {
		return "", errors.New("accepted source tree differs from persisted deployment evidence")
	}
	return tree, nil
}

// renderedDigests renders source at tree with the shared desired-state
// function and returns its render and environment digests.
func renderedDigests(ctx context.Context, store rendered.ObjectStore, tree string, env render.Env, catalog *render.Catalog, source *contracts.DeploymentSource) (rendered.DesiredSet, error) {
	if catalog == nil || source == nil || env.Namespace == "" {
		return rendered.DesiredSet{}, errors.New("a renderer catalog, namespace and deployment source are required to digest the deployment")
	}
	effective, err := rendered.Effective(ctx, store, rendered.BaseTree(tree))
	if err != nil {
		return rendered.DesiredSet{}, err
	}
	fsys, err := rendered.GitTreeFS(ctx, store, effective)
	if err != nil {
		return rendered.DesiredSet{}, err
	}
	return rendered.Desired(ctx, fsys, env, catalog, source)
}

// verifyRendered re-renders the receipt's source at tree and requires the
// recorded digests; a mismatch is *RendererChangedError, not drift.
func verifyRendered(ctx context.Context, store rendered.ObjectStore, tree string, env render.Env, catalog *render.Catalog, prior lifecycle.DeploymentReceipt) error {
	if prior.Source == nil || prior.RenderDigest == "" || prior.EnvDigest == "" {
		return errors.New("deployment evidence names no source or digests")
	}
	set, err := renderedDigests(ctx, store, tree, env, catalog, prior.Source)
	if err != nil {
		return fmt.Errorf("re-render accepted source: %w", err)
	}
	if set.RenderDigest != prior.RenderDigest {
		return &RendererChangedError{What: "render", Want: prior.RenderDigest, Got: set.RenderDigest}
	}
	if set.EnvDigest != prior.EnvDigest {
		return &RendererChangedError{What: "env", Want: prior.EnvDigest, Got: set.EnvDigest}
	}
	return nil
}
