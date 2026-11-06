package deployment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/renderenv"
	"sdo.dev/operator/sdk"
)

func desiredOf(t *testing.T, d Deployer, root string, plan *contracts.DeploymentPlan) rendered.DesiredSet {
	t.Helper()
	base, err := d.TreeDigest(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	effective, err := rendered.Effective(context.Background(), rendered.ObjectStore{Dir: root, Git: d.Git}, rendered.BaseTree(base))
	if err != nil {
		t.Fatal(err)
	}
	defer effective.Close()
	fsys, err := rendered.GitTreeFS(context.Background(), effective)
	if err != nil {
		t.Fatal(err)
	}
	set, err := rendered.Desired(context.Background(), fsys, renderenv.For(d.Environment, d.Namespace, render.SecretsOmit), d.Renderers, plan.GetSource())
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func statusOf(checks []operability.Check, id string) operability.Check {
	for _, c := range checks {
		if c.ID == id {
			return c
		}
	}
	return operability.Check{}
}

// A real deployment produces a receipt every live check passes, and the checks
// are exactly the declared set so a report can require them by name.
func TestCheckLivePassesForARealDeployment(t *testing.T) {
	root, source := depRawFixture(t)
	plan := depPlan(t, source, depRawWeb)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	deployer.Environment = &contracts.RenderEnvironment{KubeVersion: "v1.34.0", ApiVersions: []string{"apps/v1", "apps/v1/Deployment", "v1", "v1/ConfigMap", "v1/Service", "v1/Secret"}}
	receipt, err := deployer.ApplyAndVerify(context.Background(), root, plan)
	if err != nil {
		t.Fatal(err)
	}
	checks, err := deployer.CheckLive(context.Background(), root, plan, receipt, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, c := range checks {
		ids = append(ids, c.ID)
		// The fake cluster reports no per-container images, so O2.inputs is a note.
		want := operability.StatusPass
		if c.ID == "O2.inputs" {
			want = operability.StatusNote
		}
		if c.Status != want || c.Phase != operability.PhaseLive {
			t.Errorf("%s = %+v, want a passing live check", c.ID, c)
		}
	}
	if strings.Join(ids, ",") != strings.Join(LiveCheckIDs(), ",") {
		t.Fatalf("checks %v, want %v", ids, LiveCheckIDs())
	}
}

// Each receipt defect fails exactly its own check with an attribution or
// rollout cause; none can be hidden by the others passing.
func TestLiveChecksFailPerReceiptDefect(t *testing.T) {
	root, source := depRawFixture(t)
	plan := depPlan(t, source, depRawWeb)
	deployer, _ := depDeployer(t, root, &clusterFake{})
	good, err := deployer.ApplyAndVerify(context.Background(), root, plan)
	if err != nil {
		t.Fatal(err)
	}
	digest := "web@sha256:" + strings.Repeat("a", 64)
	good.ContainerImages = []rendered.DeployedImage{{
		Ref: sdk.ObjectRef{Kind: "Deployment", Namespace: "demo", Name: "web"}, Name: "web", Applied: digest, Digest: digest,
		Inputs: rendered.ImageInputs{SourceImage: "web:1"},
	}}
	if c := statusOf(LiveChecks(LiveInput{Receipt: good, Desired: desiredOf(t, deployer, root, plan)}), "O2.inputs"); c.Status != operability.StatusPass {
		t.Fatalf("a receipt with valid inputs must pass O2.inputs: %+v", c)
	}
	for name, tc := range map[string]struct {
		mutate func(*lifecycle.DeploymentReceipt)
		want   string
		cause  operability.Cause
	}{
		"not ready":          {func(r *lifecycle.DeploymentReceipt) { r.Ready = false }, "O1.ready", operability.CauseRollout},
		"stamp differs":      {func(r *lifecycle.DeploymentReceipt) { r.ObservedRevision = "other" }, "O2.stamps", operability.CauseAttribution},
		"tag instead of pin": {func(r *lifecycle.DeploymentReceipt) { r.Images = []string{"web:1"} }, "O2.pins", operability.CauseAttribution},
		"container tag": {func(r *lifecycle.DeploymentReceipt) {
			r.ContainerImages = append([]rendered.DeployedImage(nil), r.ContainerImages...)
			r.ContainerImages[0].Applied = "web:1"
		}, "O2.pins", operability.CauseAttribution},
		"no inputs": {func(r *lifecycle.DeploymentReceipt) {
			r.ContainerImages = append([]rendered.DeployedImage(nil), r.ContainerImages...)
			r.ContainerImages[0].Inputs = rendered.ImageInputs{}
		}, "O2.inputs", operability.CauseAttribution},
		"extra resource": {func(r *lifecycle.DeploymentReceipt) {
			r.ResourceIDs = append(append([]string(nil), r.ResourceIDs...), "Deployment/demo/ghost")
		}, "O2.resources", operability.CauseAttribution},
		"missing resource": {func(r *lifecycle.DeploymentReceipt) { r.ResourceIDs = nil }, "O2.resources", operability.CauseAttribution},
	} {
		t.Run(name, func(t *testing.T) {
			receipt := good
			tc.mutate(&receipt)
			checks, err := deployer.CheckLive(context.Background(), root, plan, receipt, nil)
			if err != nil {
				t.Fatal(err)
			}
			c := statusOf(checks, tc.want)
			if c.Status != operability.StatusFail || c.Cause != tc.cause {
				t.Fatalf("%s = %+v", tc.want, c)
			}
		})
	}
}

// The error taxonomy: causes come from error types, so wrapping or rewording a
// message cannot change who must act.
func TestClassifyDeploymentError(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want operability.Cause
	}{
		"renderer changed":        {&RendererChangedError{What: "render"}, operability.CauseEnvironment},
		"patch digest changed":    {&PatchSetChangedError{What: "patch series digest"}, operability.CauseAttribution},
		"patch no longer applies": {&PatchSetChangedError{What: "patch application", Err: &patchset.PatchError{}}, operability.CauseAttribution},
		"patch git unavailable":   {&PatchSetChangedError{What: "patch application", Err: &rendered.SourceError{Kind: rendered.SourceUnavailable, Err: errors.New("io")}}, operability.CauseInfrastructure},
		"patch error":             {&patchset.PatchError{}, operability.CausePatch},
		"image timeout":           {fmt.Errorf("resolve images: %w", &imagesrc.TimeoutError{Image: "a", Limit: time.Second, Budget: true}), operability.CauseInfrastructure},
		"image error":             {fmt.Errorf("resolve images: %w", &imagesrc.Error{Image: "a", SourceID: "s", Err: errors.New("bad")}), operability.CauseBuild},
		"progressing rollout":     {&RolloutTimeoutError{Resource: "Deployment/a/b"}, operability.CauseInfrastructure},
		"stalled rollout":         {fmt.Errorf("phase: %w", &RolloutTimeoutError{Resource: "Deployment/a/b", Stalled: true}), operability.CauseRollout},
		"invalid source":          {&rendered.SourceError{Kind: rendered.SourceInvalid, Err: errors.New("x")}, operability.CauseRender},
		"canceled":                {context.Canceled, operability.CauseInfrastructure},
		"unknown":                 {errors.New("boom"), operability.CauseInternal},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ClassifyDeploymentError(tc.err); got != tc.want {
				t.Fatalf("cause = %q, want %q", got, tc.want)
			}
		})
	}
}

// A failed deployment records one failing O1.deploy and skips the checks that
// read the receipt; a budget exhaustion is infrastructure, never a correction.
func TestLiveChecksForAFailedDeployment(t *testing.T) {
	checks := LiveChecks(LiveInput{Err: &RolloutTimeoutError{Resource: "Deployment/a/b", Stalled: true}})
	if len(checks) != len(LiveCheckIDs()) {
		t.Fatalf("checks = %+v", checks)
	}
	if c := checks[0]; c.ID != "O1.deploy" || c.Status != operability.StatusFail || c.Cause != operability.CauseRollout {
		t.Fatalf("deploy = %+v", c)
	}
	for _, c := range checks[1:] {
		if c.Status != operability.StatusSkipped {
			t.Errorf("%s = %+v, want skipped", c.ID, c)
		}
	}
	budget := LiveChecks(LiveInput{Err: &imagesrc.TimeoutError{Image: "a", Limit: time.Second, Budget: true}})
	if c := statusOf(budget, "O6.budget"); c.Status != operability.StatusFail || c.Cause != operability.CauseInfrastructure {
		t.Fatalf("budget = %+v", c)
	}
	report, err := operability.New(operability.StageDeployment, operability.Subject{}.WithUnavailable()).With(budget...)
	if err != nil {
		t.Fatal(err)
	}
	if d := report.Decide(operability.O1, operability.O2, operability.O6); d.Kind != operability.Retry {
		t.Fatalf("decision = %+v, want retry", d)
	}
}

// O1.apis is part of the live report: it passes with a recorded discovery that
// serves every rendered kind, is not evaluated (skipped, never silently absent)
// when no discovery was recorded, and fails with the render cause when the
// deployer refused the set for an unserved API.
func TestLiveChecksReportServedAPIs(t *testing.T) {
	unknown := statusOf(LiveChecks(LiveInput{}), "O1.apis")
	if unknown.ID == "" || unknown.Status != operability.StatusSkipped {
		t.Fatalf("no discovery recorded: %+v", unknown)
	}
	refused := LiveChecks(LiveInput{Err: &rendered.UnservedAPIError{Objects: []string{"policy/v1beta1 PodDisruptionBudget app/web"}}})
	if c := statusOf(refused, "O1.apis"); c.Status != operability.StatusFail || c.Cause != operability.CauseRender {
		t.Fatalf("unserved API: %+v", c)
	}
	if c := statusOf(refused, "O1.deploy"); c.Status != operability.StatusFail || c.Cause != operability.CauseRender {
		t.Fatalf("deploy: %+v", c)
	}
	other := LiveChecks(LiveInput{Err: &RolloutTimeoutError{Resource: "Deployment/a/b", Stalled: true}})
	if c := statusOf(other, "O1.apis"); c.Status != operability.StatusSkipped {
		t.Fatalf("other failure: %+v", c)
	}
}
