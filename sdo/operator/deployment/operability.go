package deployment

import (
	"context"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability/offline"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

var _ lifecycle.OfflineChecker = Deployer{}

// CheckOffline runs the offline operability checks for the checkout and plan
// with exactly what ApplyAndVerify would use: the checkout's proposal tree, the
// effective tree (base plus patches), the install render environment and the
// shared render and image-declaration functions. A nil plan, or one without a
// source, derives the source and builds from the tree's files. It touches
// no cluster. Errors are infrastructure or wiring problems; a bad patch,
// render or plan is a failing check.
func (d Deployer) CheckOffline(ctx context.Context, checkout string, plan *contracts.DeploymentPlan) (lifecycle.OfflineChecks, error) {
	if err := d.validate(); err != nil {
		return lifecycle.OfflineChecks{}, err
	}
	base, err := d.TreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.OfflineChecks{}, err
	}
	out := lifecycle.OfflineChecks{BaseTree: base}
	effective, patchesErr := rendered.Effective(ctx, rendered.ObjectStore{Dir: checkout, Git: d.Git}, rendered.BaseTree(base))
	if patchesErr == nil {
		defer effective.Close()
		out.EffectiveTree, out.PatchesDigest = effective.ID, effective.PatchesDigest
	}
	// SecretsOmit renders the same RenderDigest and EnvDigest as the
	// SecretsInclude render apply records (both reduce Secrets to identity).
	env := renderenv.For(d.Environment, d.Namespace, render.SecretsOmit)
	result := offline.CheckDeployment(ctx, offline.Deployment{
		PatchesErr: patchesErr,
		Render: func(ctx context.Context) (rendered.DesiredSet, error) {
			fsys, err := rendered.GitTreeFS(ctx, effective)
			if err != nil {
				return rendered.DesiredSet{}, err
			}
			return rendered.Desired(ctx, fsys, env, d.Renderers, plan.GetSource())
		},
		ServedAPIs: d.Environment.GetApiVersions(),
		Declare: func(set rendered.DesiredSet) ([]imagesrc.Declared, error) {
			fsys, err := rendered.GitTreeFS(ctx, effective)
			if err != nil {
				return nil, err
			}
			return d.declare(set, plan, fsys)
		},
		Policy: deploymentpolicy.Offline(d.Policy),
	})
	out.Checks = result.Checks
	out.RenderDigest, out.EnvDigest = result.Set.RenderDigest, result.Set.EnvDigest
	out.UnusedImages = result.UnusedImages
	return out, nil
}
