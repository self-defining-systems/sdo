package deployment

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
)

var _ lifecycle.LiveChecker = Deployer{}

// LiveInput is what the live deployment checks read: the outcome of
// ApplyAndVerify (or VerifyAccepted), the desired set of the effective tree and,
// when the outcome is a failure, the error.
type LiveInput struct {
	Receipt lifecycle.DeploymentReceipt
	Desired rendered.DesiredSet
	// ServedAPIs is the recorded discovery the deployer checked the desired set
	// against; empty means no discovery was recorded.
	ServedAPIs []string
	// Err is the ApplyAndVerify or VerifyAccepted error; nil means the receipt is trusted.
	Err error
}

// LiveCheckIDs lists the live checks in the order LiveChecks reports them.
func LiveCheckIDs() []string {
	return []string{"O1.deploy", "O1.ready", "O1.apis", "O2.stamps", "O2.pins", "O2.inputs", "O2.resources", "O6.budget"}
}

// ClassifyDeploymentError maps an error of ApplyAndVerify or VerifyAccepted to a
// cause, by type and never by message: cancellation, rate limits, timeouts and
// still-progressing rollouts are infrastructure; an unresolvable image is a
// build fault; a stalled rollout is a rollout fault; a patch series or renderer
// that no longer reproduces what was deployed is attribution or environment, not
// the application's; unknown shapes are internal defects.
func ClassifyDeploymentError(err error) operability.Cause {
	if err == nil {
		return ""
	}
	var rendererChanged *RendererChangedError
	var patchChanged *PatchSetChangedError
	var patchErr *patchset.PatchError
	var timeout *imagesrc.TimeoutError
	var rollout *RolloutTimeoutError
	var imageErr *imagesrc.Error
	var source *rendered.SourceError
	var policy *PolicyError
	var immutable *ImmutableChangeError
	var unserved *rendered.UnservedAPIError
	switch {
	case errors.As(err, &unserved):
		return operability.CauseRender
	case errors.As(err, &policy):
		return policy.Checks[0].Cause
	case errors.As(err, &immutable):
		return immutable.Cause()
	case errors.As(err, &rendererChanged):
		return operability.CauseEnvironment
	case errors.As(err, &patchChanged):
		if patchChanged.Err != nil && !errors.As(patchChanged.Err, &patchErr) {
			return offline.ClassifyRender(patchChanged.Err, operability.CauseEnvironment)
		}
		return operability.CauseAttribution
	case errors.As(err, &patchErr):
		return operability.CausePatch
	case errors.As(err, &timeout), lifecycle.IsInfrastructureOutcome(err):
		return operability.CauseInfrastructure
	case errors.As(err, &rollout):
		return operability.CauseRollout
	case errors.As(err, &imageErr):
		return operability.CauseBuild
	case errors.As(err, &source):
		return offline.ClassifyRender(err, operability.CauseRender)
	}
	return offline.ClassifyRender(err, operability.CauseInternal)
}

// LiveChecks translates the outcome of a deployment into the live checks of O1,
// O2 and O6. It is pure. A failed outcome records O1.deploy with the classified
// cause and skips the checks that read the receipt.
func LiveChecks(in LiveInput) []operability.Check {
	live := func(id string, status operability.Status, cause operability.Cause, detail string, paths ...string) operability.Check {
		return operability.Check{ID: id, Phase: operability.PhaseLive, Status: status, Cause: cause, Detail: operability.Clip(detail), Paths: paths}
	}
	pass := func(id, detail string) operability.Check { return live(id, operability.StatusPass, "", detail) }
	fail := func(id string, cause operability.Cause, detail string) operability.Check {
		return live(id, operability.StatusFail, cause, detail)
	}
	r := in.Receipt

	var checks []operability.Check
	budget := budgetCheck(in.Err)
	if in.Err != nil {
		checks = append(checks, fail("O1.deploy", ClassifyDeploymentError(in.Err), in.Err.Error()))
		for _, id := range LiveCheckIDs()[1:] {
			if id == budget.ID && budget.Status != operability.StatusSkipped {
				checks = append(checks, budget)
				continue
			}
			if id == "O1.apis" {
				checks = append(checks, apisCheck(in))
				continue
			}
			checks = append(checks, live(id, operability.StatusSkipped, "", "not run: O1.deploy failed"))
		}
		return checks
	}
	checks = append(checks, pass("O1.deploy", "applied and verified"+recreatedDetail(r.Recreated)))

	if !r.Ready || len(r.ResourceIDs) == 0 {
		checks = append(checks, fail("O1.ready", operability.CauseRollout, "the receipt records no ready workloads"))
	} else {
		checks = append(checks, pass("O1.ready", fmt.Sprintf("%d workloads ready", len(r.ResourceIDs))))
	}

	checks = append(checks, apisCheck(in))

	if r.EffectiveTree == "" || r.ObservedRevision != r.EffectiveTree {
		checks = append(checks, fail("O2.stamps", operability.CauseAttribution, fmt.Sprintf("observed source stamp %q is not the effective tree %q", r.ObservedRevision, r.EffectiveTree)))
	} else {
		checks = append(checks, pass("O2.stamps", "every workload carries the effective tree stamp"))
	}

	checks = append(checks, pinsCheck(r), inputsCheck(r), resourcesCheck(r, in.Desired), budget)
	return checks
}

// apisCheck reports O1.apis: the deployer refuses a desired set whose kinds the
// recorded discovery does not serve, before applying anything. With no
// recorded discovery the check is not evaluated, and says so.
func apisCheck(in LiveInput) operability.Check {
	c := operability.Check{ID: "O1.apis", Phase: operability.PhaseLive}
	var unserved *rendered.UnservedAPIError
	switch {
	case errors.As(in.Err, &unserved):
		c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseRender, operability.Clip(unserved.Error())
	case in.Err != nil:
		c.Status, c.Detail = operability.StatusSkipped, "not run: O1.deploy failed"
	case len(in.ServedAPIs) == 0:
		c.Status, c.Detail = operability.StatusSkipped, "not run: no cluster discovery was recorded"
	default:
		if err := rendered.CheckServedAPIs(in.Desired, in.ServedAPIs); err != nil {
			c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseRender, operability.Clip(err.Error())
		} else {
			c.Status, c.Detail = operability.StatusPass, "rendered objects use API versions the cluster serves"
		}
	}
	return c
}

func pinsCheck(r lifecycle.DeploymentReceipt) operability.Check {
	var bad []string
	for _, image := range r.Images {
		if !digestReference.MatchString(image) {
			bad = append(bad, image)
		}
	}
	for _, image := range r.ContainerImages {
		if !digestReference.MatchString(image.Applied) {
			bad = append(bad, image.Ref.Name+"/"+image.Name+"="+image.Applied)
		}
	}
	c := operability.Check{ID: "O2.pins", Phase: operability.PhaseLive}
	switch {
	case len(r.Images) == 0:
		c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseAttribution, "the receipt records no images"
	case len(bad) > 0:
		c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseAttribution, "images not pinned by digest: "+list(bad)
	default:
		c.Status, c.Detail = operability.StatusPass, fmt.Sprintf("%d images pinned by digest", len(r.Images))
	}
	c.Detail = operability.Clip(c.Detail)
	return c
}

func inputsCheck(r lifecycle.DeploymentReceipt) operability.Check {
	c := operability.Check{ID: "O2.inputs", Phase: operability.PhaseLive}
	if len(r.ContainerImages) == 0 {
		// A backend that reports only a flat image list records no per-container inputs.
		c.Status, c.Detail = operability.StatusNote, "the receipt records no per-container images"
		return c
	}
	var bad []string
	for _, image := range r.ContainerImages {
		if image.Inputs.SourceImage == "" || image.Inputs.Validate() != nil {
			bad = append(bad, image.Ref.Name+"/"+image.Name)
		}
	}
	if len(bad) > 0 {
		c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseAttribution, "images without valid recorded inputs: "+list(bad)
	} else {
		c.Status, c.Detail = operability.StatusPass, fmt.Sprintf("%d container images have recorded inputs", len(r.ContainerImages))
	}
	c.Detail = operability.Clip(c.Detail)
	return c
}

// desiredWorkloadIDs are the identities apply records for a receipt: the
// workloads of non-ephemeral phases.
func desiredWorkloadIDs(set rendered.DesiredSet) []string {
	var ids []string
	for _, phase := range set.Phases {
		if phase.Ephemeral {
			continue
		}
		for _, object := range phase.Objects {
			if object.Workload {
				ids = append(ids, object.Ref.Kind+"/"+object.Ref.Namespace+"/"+object.Ref.Name)
			}
		}
	}
	sort.Strings(ids)
	return ids
}

func resourcesCheck(r lifecycle.DeploymentReceipt, set rendered.DesiredSet) operability.Check {
	c := operability.Check{ID: "O2.resources", Phase: operability.PhaseLive}
	want := desiredWorkloadIDs(set)
	have := append([]string(nil), r.ResourceIDs...)
	sort.Strings(have)
	wantSet, haveSet := map[string]bool{}, map[string]bool{}
	for _, id := range want {
		wantSet[id] = true
	}
	for _, id := range have {
		haveSet[id] = true
	}
	var missing, extra []string
	for _, id := range want {
		if !haveSet[id] {
			missing = append(missing, id)
		}
	}
	for _, id := range have {
		if !wantSet[id] {
			extra = append(extra, id)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		c.Status, c.Detail = operability.StatusPass, fmt.Sprintf("%d recorded workloads equal the desired workloads", len(have))
		return c
	}
	c.Status, c.Cause = operability.StatusFail, operability.CauseAttribution
	var parts []string
	if len(missing) > 0 {
		parts = append(parts, "missing from the receipt: "+list(missing))
	}
	if len(extra) > 0 {
		parts = append(parts, "not in the desired set: "+list(extra))
	}
	c.Detail = operability.Clip(strings.Join(parts, "; "))
	return c
}

// budgetCheck records whether a time budget ran out. Exhaustion stays an
// infrastructure outcome (retry), never a correction for an agent; with no
// error it passes, and with another error it was not evaluated.
func budgetCheck(err error) operability.Check {
	c := operability.Check{ID: "O6.budget", Phase: operability.PhaseLive}
	var timeout *imagesrc.TimeoutError
	switch {
	case err == nil:
		c.Status, c.Detail = operability.StatusPass, "no build or rollout budget was exhausted"
	case errors.As(err, &timeout):
		c.Status, c.Cause, c.Detail = operability.StatusFail, operability.CauseInfrastructure, operability.Clip(timeout.Error())
	default:
		c.Status, c.Detail = operability.StatusSkipped, "not run: O1.deploy failed"
	}
	return c
}

func list(names []string) string {
	const limit = 5
	if len(names) > limit {
		return strings.Join(names[:limit], ", ") + fmt.Sprintf(" and %d more", len(names)-limit)
	}
	return strings.Join(names, ", ")
}

// CheckLive computes the live checks for the outcome of ApplyAndVerify on the
// checkout and plan: it re-renders the effective tree with the apply
// environment (no cluster) to know the desired workloads. outcome is the
// ApplyAndVerify error; a nil outcome trusts receipt. The returned error is a
// wiring problem, not a failing check.
func (d Deployer) CheckLive(ctx context.Context, checkout string, plan *contracts.DeploymentPlan, receipt lifecycle.DeploymentReceipt, outcome error) ([]operability.Check, error) {
	if outcome != nil {
		return LiveChecks(LiveInput{Receipt: receipt, Err: outcome}), nil
	}
	if err := d.validate(); err != nil {
		return nil, err
	}
	if plan == nil || plan.GetSource() == nil {
		return nil, errors.New("deployment plan needs a source")
	}
	base, err := d.TreeDigest(ctx, checkout)
	if err != nil {
		return nil, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: checkout, Git: d.Git}, rendered.BaseTree(base))
	if err != nil {
		return LiveChecks(LiveInput{Receipt: receipt, Err: err}), nil
	}
	defer effective.Close()
	fsys, err := rendered.GitTreeFS(ctx, effective)
	if err != nil {
		return nil, err
	}
	set, err := rendered.Desired(ctx, fsys, renderenv.For(d.Environment, d.Namespace, render.SecretsOmit), d.Renderers, plan.GetSource())
	if err != nil {
		return LiveChecks(LiveInput{Receipt: receipt, Err: err}), nil
	}
	return LiveChecks(LiveInput{Receipt: receipt, Desired: set, ServedAPIs: d.Environment.GetApiVersions()}), nil
}

// recreatedDetail names the objects the deployer recreated for an immutable
// change, so the operability report shows every recreate.
func recreatedDetail(recreated []rendered.Recreation) string {
	if len(recreated) == 0 {
		return ""
	}
	parts := make([]string, 0, len(recreated))
	for _, rec := range recreated {
		part := fmt.Sprintf("%s/%s/%s (%s: %s, %s)", rec.Ref.Kind, rec.Ref.Namespace, rec.Ref.Name, rec.Reason, strings.Join(rec.Fields, ","), rec.Propagation)
		if len(rec.Replaced) > 0 {
			part += " replaced " + strings.Join(rec.Replaced, ",")
		}
		parts = append(parts, part)
	}
	return "; recreated " + strings.Join(parts, "; ")
}
