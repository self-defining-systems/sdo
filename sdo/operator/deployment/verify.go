package deployment

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
func (d Deployer) VerifyAccepted(ctx context.Context, commit, namespace string, prior lifecycle.DeploymentReceipt, evidence Evidence) (lifecycle.DeploymentReceipt, error) {
	if err := d.validate(); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if d.AcceptedRoot == "" || commit == "" || namespace == "" || !prior.Ready || prior.ProposalTreeDigest == "" || len(prior.ResourceIDs) == 0 {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted repository, commit, namespace, and trusted source evidence are required")
	}
	if namespace != d.Namespace {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted source namespace differs from the deployer's namespace")
	}
	if evidence.Source == nil || evidence.BaseTree != prior.ProposalTreeDigest {
		return lifecycle.DeploymentReceipt{}, errors.New("deployment evidence does not match the receipt's source tree")
	}
	head, err := d.git(ctx, "rev-parse", "HEAD")
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if _, err := d.git(ctx, "merge-base", "--is-ancestor", commit, head); err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("source commit is not in the accepted branch: %w", err)
	}
	tree, err := d.git(ctx, "rev-parse", commit+"^{tree}")
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	if tree != prior.ProposalTreeDigest || prior.ObservedRevision != tree {
		return lifecycle.DeploymentReceipt{}, errors.New("accepted source tree differs from persisted deployment evidence")
	}
	store := rendered.ObjectStore{Dir: d.AcceptedRoot, Git: d.Git}
	effective, err := rendered.Effective(ctx, store, rendered.BaseTree(tree))
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	fsys, err := rendered.GitTreeFS(ctx, store, effective)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	set, err := rendered.Desired(ctx, fsys, d.env(render.SecretsOmit), d.Renderers, evidence.Source)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, fmt.Errorf("re-render accepted source: %w", err)
	}
	if set.RenderDigest != evidence.RenderDigest {
		return lifecycle.DeploymentReceipt{}, &RendererChangedError{What: "render", Want: evidence.RenderDigest, Got: set.RenderDigest}
	}
	if set.EnvDigest != evidence.EnvDigest {
		return lifecycle.DeploymentReceipt{}, &RendererChangedError{What: "env", Want: evidence.EnvDigest, Got: set.EnvDigest}
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
		if !observed.Ready || observed.ResourceID != id || observed.Revision != prior.ProposalTreeDigest {
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
