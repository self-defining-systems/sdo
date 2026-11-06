package deployment

import (
	"context"
	"errors"
	"fmt"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment/internal/deploymentio"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
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

// PatchSetChangedError reports that the accepted commit's deployment patch
// series no longer yields the effective tree that was deployed: a patch was
// edited or removed outside the broker's evidence, or applying the same patches
// now gives a different tree (a git behavior change). It is neither drift of
// the running application nor a renderer change.
type PatchSetChangedError struct {
	// What names the difference: "patch series digest", "effective tree" or
	// "patch application".
	What string
	Want string
	Got  string
	// Err is the application failure when What is "patch application".
	Err error
}

func (e *PatchSetChangedError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("accepted deployment patches no longer apply to the accepted source (they applied when it was deployed): %v", e.Err)
	}
	return fmt.Sprintf("accepted deployment patches yield a different %s than when it was deployed (%s, now %s): the patch set or patch application changed, not the running application", e.What, e.Want, e.Got)
}

func (e *PatchSetChangedError) Unwrap() error { return e.Err }

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

// CheckAcceptedPolicy re-renders the recorded accepted deployment and checks it
// against the source evidence and the policy in force (a *PolicyError when it
// violates it), without touching the cluster. VerifyAccepted runs it first; a
// controller start runs it alone, so a restart under a tightened policy is
// found whether or not a health refresh is due.
func (d Deployer) CheckAcceptedPolicy(ctx context.Context, commit, namespace string, prior lifecycle.DeploymentReceipt) error {
	if err := d.validate(); err != nil {
		return err
	}
	if d.AcceptedRoot == "" || commit == "" || namespace == "" || !prior.Ready || prior.BaseTree == "" || prior.EffectiveTree == "" || len(prior.ResourceIDs) == 0 {
		return errors.New("accepted repository, commit, namespace, and trusted source evidence are required")
	}
	if namespace != d.Namespace {
		return errors.New("accepted source namespace differs from the deployer's namespace")
	}
	tree, err := acceptedTree(ctx, d.git, commit, prior)
	if err != nil {
		return err
	}
	// The policy in force now binds the accepted deployment too: a restart
	// under a tightened policy refuses a deployment that violates it.
	for _, policy := range []*contracts.DeploymentPolicy{d.Policy, prior.Policy} {
		if err := deploymentpolicy.Validate(policy); err != nil {
			return err
		}
	}
	if err := verifyRendered(ctx, rendered.ObjectStore{Dir: d.AcceptedRoot, Git: d.Git}, tree, renderenv.For(prior.Environment, d.Namespace, render.SecretsOmit), d.Renderers, prior, d.enforcePolicy); err != nil {
		return err
	}
	return nil
}

// VerifyAccepted rechecks persisted deployment evidence after a restart: the
// commit must be in the accepted branch with exactly the deployed tree, the
// accepted source must still render to the recorded digests (otherwise
// *RendererChangedError), and every recorded workload must still be ready at
// that revision with the recorded images.
func (d Deployer) VerifyAccepted(ctx context.Context, commit, namespace string, prior lifecycle.DeploymentReceipt) (lifecycle.DeploymentReceipt, error) {
	if err := d.CheckAcceptedPolicy(ctx, commit, namespace, prior); err != nil {
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
		if !observed.Ready || observed.ResourceID != id || observed.Revision != prior.EffectiveTree {
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
// base tree the receipt deployed, and returns that tree. The effective tree is
// then recomputed by verifyRendered.
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
	if tree != prior.BaseTree || prior.ObservedRevision != prior.EffectiveTree {
		return "", errors.New("accepted source tree differs from persisted deployment evidence")
	}
	return tree, nil
}

// effectiveFor recomputes the effective tree of base and requires the patch
// series digest and tree ID the receipt recorded (*PatchSetChangedError). The
// caller closes the tree.
func effectiveFor(ctx context.Context, store rendered.ObjectStore, base string, prior lifecycle.DeploymentReceipt) (rendered.EffectiveTree, error) {
	effective, err := rendered.Effective(ctx, store, rendered.BaseTree(base))
	if err != nil {
		var patchErr *patchset.PatchError
		if errors.As(err, &patchErr) {
			return rendered.EffectiveTree{}, &PatchSetChangedError{What: "patch application", Err: err}
		}
		return rendered.EffectiveTree{}, fmt.Errorf("compute effective tree of accepted source: %w", err)
	}
	if effective.PatchesDigest != prior.PatchesDigest {
		defer effective.Close()
		return rendered.EffectiveTree{}, &PatchSetChangedError{What: "patch series digest", Want: prior.PatchesDigest, Got: effective.PatchesDigest}
	}
	if effective.ID != prior.EffectiveTree {
		defer effective.Close()
		return rendered.EffectiveTree{}, &PatchSetChangedError{What: "effective tree", Want: prior.EffectiveTree, Got: effective.ID}
	}
	return effective, nil
}

// renderedDigests renders source from the effective tree with the shared
// desired-state function and returns its render and environment digests.
func renderedDigests(ctx context.Context, effective rendered.EffectiveTree, env render.Env, catalog *render.Catalog, source *contracts.DeploymentSource) (rendered.DesiredSet, error) {
	if catalog == nil || source == nil || env.Namespace == "" {
		return rendered.DesiredSet{}, errors.New("a renderer catalog, namespace and deployment source are required to digest the deployment")
	}
	fsys, err := rendered.GitTreeFS(ctx, effective)
	if err != nil {
		return rendered.DesiredSet{}, err
	}
	return rendered.Desired(ctx, fsys, env, catalog, source)
}

// verifyRendered recomputes the effective tree of base (requiring the
// recorded patch series and tree, *PatchSetChangedError otherwise), re-renders
// the receipt's source from it and requires the recorded digests; a mismatch
// is *RendererChangedError, not drift.
func verifyRendered(ctx context.Context, store rendered.ObjectStore, base string, env render.Env, catalog *render.Catalog, prior lifecycle.DeploymentReceipt, enforce func(rendered.DesiredSet) error) error {
	if prior.Source == nil || prior.RenderDigest == "" || prior.EnvDigest == "" || prior.EffectiveTree == "" || prior.PatchesDigest == "" {
		return errors.New("deployment evidence names no source, effective tree or digests")
	}
	effective, err := effectiveFor(ctx, store, base, prior)
	if err != nil {
		return err
	}
	defer effective.Close()
	set, err := renderedDigests(ctx, effective, env, catalog, prior.Source)
	if err != nil {
		return fmt.Errorf("re-render accepted source: %w", err)
	}
	if set.RenderDigest != prior.RenderDigest {
		return &RendererChangedError{What: "render", Want: prior.RenderDigest, Got: set.RenderDigest}
	}
	if set.EnvDigest != prior.EnvDigest {
		return &RendererChangedError{What: "env", Want: prior.EnvDigest, Got: set.EnvDigest}
	}
	if enforce == nil {
		return nil
	}
	return enforce(set)
}
