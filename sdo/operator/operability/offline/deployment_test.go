package offline_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/operability"
	"sdo.dev/operator/operability/offline"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

const web = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: c, image: 'registry.example/app/web:1'}]}}}\n"

var webID = "Deployment/app/web"

// renderer builds a Render function over the real catalog render path.
func renderer(t *testing.T, files rendertest.MapFS) func(context.Context) (rendered.DesiredSet, error) {
	t.Helper()
	cat := render.NewCatalog()
	if err := raw.Register(cat); err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawRenderConfig{Paths: []string{"k8s"}})
	if err != nil {
		t.Fatal(err)
	}
	src := &contracts.DeploymentSource{RendererId: "raw", Config: config}
	return func(ctx context.Context) (rendered.DesiredSet, error) {
		return rendered.Desired(ctx, files, render.Env{Namespace: "app"}, cat, src)
	}
}

func deployment(t *testing.T, files rendertest.MapFS) offline.Deployment {
	return offline.Deployment{Render: renderer(t, files), ExpectedWorkloads: []string{webID}}
}

func byID(t *testing.T, checks []operability.Check) map[string]operability.Check {
	t.Helper()
	out := map[string]operability.Check{}
	for _, c := range checks {
		if _, dup := out[c.ID]; dup {
			t.Fatalf("duplicate check %s", c.ID)
		}
		if c.Phase != operability.PhaseOffline {
			t.Errorf("%s ran in phase %s", c.ID, c.Phase)
		}
		out[c.ID] = c
	}
	return out
}

func want(t *testing.T, got map[string]operability.Check, id string, status operability.Status, cause operability.Cause) operability.Check {
	t.Helper()
	c, ok := got[id]
	if !ok {
		t.Fatalf("no check %s in %v", id, got)
	}
	if c.Status != status || c.Cause != cause {
		t.Fatalf("%s = %s/%s (%s), want %s/%s", id, c.Status, c.Cause, c.Detail, status, cause)
	}
	return c
}

func TestCleanDeploymentPassesAndFitsTheReport(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.Policy = offline.Policy{AllowedRegistries: []string{"registry.example"}}
	res := offline.CheckDeployment(context.Background(), d)
	got := byID(t, res.Checks)
	for _, id := range []string{"O1.patches", "O1.render", "O1.deterministic", "O1.workloads", "O6.scope", "O6.allowlist"} {
		want(t, got, id, operability.StatusPass, "")
	}
	want(t, got, "O2.images", operability.StatusNote, "") // undeclared tag: default source
	if res.Set.RenderDigest == "" {
		t.Fatal("no desired set returned")
	}
	// The checks are well-formed report content for items O1, O2 and O6.
	r, err := operability.New(operability.StageDeployment, operability.Subject{
		AcceptedCommit: "c", BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p",
		RenderDigest: res.Set.RenderDigest, EnvDigest: res.Set.EnvDigest,
	}).With(res.Checks...)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if dec := r.Decide(operability.StageItems(operability.StageDeployment)...); dec.Kind != operability.Accepted {
		t.Fatalf("decision = %+v", dec)
	}
}

func TestPatchFailuresAreTheDeployersAndStopTheSequence(t *testing.T) {
	cases := map[string]struct {
		err   error
		cause operability.Cause
		path  string
	}{
		"conflict": {
			&rendered.SourceError{Kind: rendered.SourceInvalid, Err: &patchset.PatchError{Kind: patchset.KindConflict, Patch: "0001-x.patch", Paths: []string{"k8s/web.yaml"}, Detail: "context does not match"}},
			operability.CausePatch, ".sdo/deployment/patches/0001-x.patch",
		},
		"unavailable": {&rendered.SourceError{Kind: rendered.SourceUnavailable, Err: errors.New("git died")}, operability.CauseInfrastructure, ""},
		"cancelled":   {context.Canceled, operability.CauseInfrastructure, ""},
		"unknown":     {errors.New("bad tree"), operability.CauseInternal, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
			d.PatchesErr = tc.err
			res := offline.CheckDeployment(context.Background(), d)
			got := byID(t, res.Checks)
			if len(got) != 8 {
				t.Fatalf("checks after a patch failure: %+v", res.Checks)
			}
			for id, c := range got {
				if id != "O1.patches" && c.Status != operability.StatusSkipped {
					t.Fatalf("%s ran after a patch failure: %+v", id, c)
				}
			}
			c := want(t, got, "O1.patches", operability.StatusFail, tc.cause)
			if tc.path != "" && (len(c.Paths) == 0 || c.Paths[0] != tc.path) {
				t.Fatalf("paths = %v", c.Paths)
			}
		})
	}
}

func TestRenderFailureClassification(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/bad.yaml": "kind: [unterminated\n"})
	got := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.render", operability.StatusFail, operability.CauseRender)
	want(t, got, "O1.deterministic", operability.StatusSkipped, "")
	want(t, got, "O6.allowlist", operability.StatusSkipped, "")
	if len(got) != 8 {
		t.Fatalf("render failure must skip later checks: %v", got)
	}
	d.Render = func(context.Context) (rendered.DesiredSet, error) {
		return rendered.DesiredSet{}, &rendered.SourceError{Kind: rendered.SourceUnavailable, Err: errors.New("io")}
	}
	got = byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.render", operability.StatusFail, operability.CauseInfrastructure)
	d.Render = nil
	got = byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.render", operability.StatusFail, operability.CauseInternal)
}

// Negative control: a renderer whose output changes between calls.
func TestNondeterministicRenderFails(t *testing.T) {
	inner := renderer(t, rendertest.MapFS{"k8s/web.yaml": web})
	calls := 0
	d := offline.Deployment{ExpectedWorkloads: []string{webID}, Render: func(ctx context.Context) (rendered.DesiredSet, error) {
		set, err := inner(ctx)
		calls++
		if calls == 2 {
			set.RenderDigest = "drifted"
		}
		return set, err
	}}
	got := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	c := want(t, got, "O1.deterministic", operability.StatusFail, operability.CauseRenderNondeterministic)
	if !strings.Contains(c.Detail, "render") || calls != 2 {
		t.Fatalf("detail %q calls %d", c.Detail, calls)
	}
}

func TestSecondRenderFailureIsNondeterminism(t *testing.T) {
	inner := renderer(t, rendertest.MapFS{"k8s/web.yaml": web})
	calls := 0
	d := offline.Deployment{ExpectedWorkloads: []string{webID}, Render: func(ctx context.Context) (rendered.DesiredSet, error) {
		calls++
		if calls == 2 {
			return rendered.DesiredSet{}, &rendered.SourceError{Kind: rendered.SourceInvalid, Err: errors.New("flaked")}
		}
		return inner(ctx)
	}}
	want(t, byID(t, offline.CheckDeployment(context.Background(), d).Checks), "O1.deterministic", operability.StatusFail, operability.CauseRenderNondeterministic)
}

func TestExpectedWorkloadsMismatch(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.ExpectedWorkloads = []string{webID, "Deployment/app/ghost"}
	got := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.workloads", operability.StatusFail, operability.CauseRender)
	// The check is the shared function, not a copy.
	set, _ := d.Render(context.Background())
	if err := rendered.CheckExpectedWorkloads(set, d.ExpectedWorkloads); err == nil || !strings.Contains(got["O1.workloads"].Detail, err.Error()) {
		t.Fatalf("detail %q does not carry the shared error %v", got["O1.workloads"].Detail, err)
	}
}

func TestImageDeclarations(t *testing.T) {
	files := rendertest.MapFS{"k8s/web.yaml": web}
	for name, tc := range map[string]struct {
		declared []imagesrc.Declared
		status   operability.Status
		cause    operability.Cause
	}{
		"declared":       {[]imagesrc.Declared{{Image: "registry.example/app/web:1", SourceID: "buildkit"}}, operability.StatusPass, ""},
		"unused":         {[]imagesrc.Declared{{Image: "registry.example/app/other:1", SourceID: "buildkit"}}, operability.StatusFail, operability.CauseBuild},
		"declared twice": {[]imagesrc.Declared{{Image: "registry.example/app/web:1", SourceID: "a"}, {Image: "registry.example/app/web:1", SourceID: "b"}}, operability.StatusFail, operability.CauseBuild},
	} {
		t.Run(name, func(t *testing.T) {
			d := deployment(t, files)
			d.Declared = tc.declared
			want(t, byID(t, offline.CheckDeployment(context.Background(), d).Checks), "O2.images", tc.status, tc.cause)
		})
	}
	pinned := strings.Replace(web, "registry.example/app/web:1", "registry.example/app/web@sha256:"+strings.Repeat("a", 64), 1)
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": pinned})
	want(t, byID(t, offline.CheckDeployment(context.Background(), d).Checks), "O2.images", operability.StatusPass, "")
}

const (
	foreignSvc  = "apiVersion: v1\nkind: Service\nmetadata: {name: other, namespace: elsewhere}\n"
	clusterRole = "apiVersion: rbac.authorization.k8s.io/v1\nkind: ClusterRole\nmetadata: {name: reader}\n"
)

func TestScopeIsANoteUntilBlockingThenFailsUnlessGranted(t *testing.T) {
	files := rendertest.MapFS{"k8s/web.yaml": web, "k8s/f.yaml": foreignSvc, "k8s/c.yaml": clusterRole}
	run := func(p offline.Policy) operability.Check {
		d := deployment(t, files)
		d.Policy = p
		return byID(t, offline.CheckDeployment(context.Background(), d).Checks)["O6.scope"]
	}
	c := run(offline.Policy{})
	if c.Status != operability.StatusNote || !strings.Contains(c.Detail, "elsewhere/Service/other") || !strings.Contains(c.Detail, "ClusterRole/reader") {
		t.Fatalf("default scope = %+v", c)
	}
	if c := run(offline.Policy{ScopeBlocking: true}); c.Status != operability.StatusFail || c.Cause != operability.CauseScope {
		t.Fatalf("blocking scope = %+v", c)
	}
	if c := run(offline.Policy{ScopeBlocking: true, GrantForeign: true}); c.Status != operability.StatusFail || strings.Contains(c.Detail, "foreign") {
		t.Fatalf("foreign granted but cluster not: %+v", c)
	}
	if c := run(offline.Policy{ScopeBlocking: true, GrantForeign: true, GrantClusterScoped: true}); c.Status != operability.StatusPass {
		t.Fatalf("all granted = %+v", c)
	}
}

func TestNamedGrantsPermitOnlyWhatTheyName(t *testing.T) {
	files := rendertest.MapFS{"k8s/web.yaml": web, "k8s/f.yaml": foreignSvc, "k8s/c.yaml": clusterRole}
	run := func(p offline.Policy) operability.Check {
		p.ScopeBlocking = true
		d := deployment(t, files)
		d.Policy = p
		return byID(t, offline.CheckDeployment(context.Background(), d).Checks)["O6.scope"]
	}
	if c := run(offline.Policy{GrantedNamespaces: []string{"elsewhere"}}); c.Status != operability.StatusFail || strings.Contains(c.Detail, "foreign") || !strings.Contains(c.Detail, "ClusterRole/reader") {
		t.Fatalf("namespace granted, cluster kind not: %+v", c)
	}
	if c := run(offline.Policy{GrantedClusterKinds: []string{"ClusterRole"}}); c.Status != operability.StatusFail || !strings.Contains(c.Detail, "elsewhere/Service/other") || strings.Contains(c.Detail, "ClusterRole") {
		t.Fatalf("cluster kind granted, namespace not: %+v", c)
	}
	if c := run(offline.Policy{GrantedNamespaces: []string{"other"}, GrantedClusterKinds: []string{"ClusterRoleBinding"}}); c.Status != operability.StatusFail {
		t.Fatalf("grants of other names must not apply: %+v", c)
	}
	if c := run(offline.Policy{GrantedNamespaces: []string{"elsewhere"}, GrantedClusterKinds: []string{"ClusterRole"}}); c.Status != operability.StatusPass {
		t.Fatalf("everything granted by name = %+v", c)
	}
}

func TestAllowlist(t *testing.T) {
	files := rendertest.MapFS{"k8s/web.yaml": web}
	d := deployment(t, files)
	if c := byID(t, offline.CheckDeployment(context.Background(), d).Checks)["O6.allowlist"]; c.Status != operability.StatusSkipped {
		t.Fatalf("unconfigured allowlist must be skipped, got %+v", c)
	}
	d.Policy.AllowedRegistries = []string{"ghcr.io"}
	c := byID(t, offline.CheckDeployment(context.Background(), d).Checks)["O6.allowlist"]
	if c.Status != operability.StatusFail || c.Cause != operability.CauseAllowlist || !strings.Contains(c.Detail, "registry.example/app/web:1") {
		t.Fatalf("outside allowlist = %+v", c)
	}
	d.Policy.AllowedRegistries = []string{"Registry.Example"}
	want(t, byID(t, offline.CheckDeployment(context.Background(), d).Checks), "O6.allowlist", operability.StatusPass, "")
}

func TestRegistryHost(t *testing.T) {
	for image, host := range map[string]string{
		"nginx:1":                     "docker.io",
		"library/nginx:1":             "docker.io",
		"ghcr.io/o/r:1":               "ghcr.io",
		"localhost/r:1":               "localhost",
		"localhost:5000/r@sha256:abc": "localhost:5000",
		"Registry.Example/a/b:2":      "registry.example",
		"example.com:443/a":           "example.com:443",
	} {
		if got := offline.RegistryHost(image); got != host {
			t.Errorf("RegistryHost(%q) = %q, want %q", image, got, host)
		}
	}
}

func TestDetailsStayWithinTheReportLimit(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.PatchesErr = &patchset.PatchError{Kind: patchset.KindForbidden, Patch: "0001-x.patch", Detail: strings.Repeat("long ", 400)}
	for _, c := range offline.CheckDeployment(context.Background(), d).Checks {
		if len(c.Detail) > operability.MaxDetailBytes || strings.ContainsAny(c.Detail, "\n") {
			t.Fatalf("%s detail is %d bytes", c.ID, len(c.Detail))
		}
	}
	var many []string
	for i := 0; i < 12; i++ {
		many = append(many, fmt.Sprintf("apiVersion: v1\nkind: Service\nmetadata: {name: s%d, namespace: ns%d}\n", i, i))
	}
	d = deployment(t, rendertest.MapFS{"k8s/web.yaml": web, "k8s/many.yaml": strings.Join(many, "---\n")})
	c := byID(t, offline.CheckDeployment(context.Background(), d).Checks)["O6.scope"]
	if !strings.Contains(c.Detail, "and 7 more") {
		t.Fatalf("detail = %q", c.Detail)
	}
}

// A failure that stops the sequence must still leave every deployment item with
// checks, so the failure decides the report instead of the silence rule.
func TestEarlyFailureDecidesTheReportNotSilence(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.PatchesErr = &rendered.SourceError{Kind: rendered.SourceInvalid, Err: &patchset.PatchError{Kind: patchset.KindConflict, Patch: "0001-x.patch", Detail: "x"}}
	res := offline.CheckDeployment(context.Background(), d)
	r, err := operability.New(operability.StageDeployment, operability.Subject{
		AcceptedCommit: "c", BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p", RenderDigest: "r", EnvDigest: "v",
	}).With(res.Checks...)
	if err != nil {
		t.Fatal(err)
	}
	dec := r.Decide(operability.StageItems(operability.StageDeployment)...)
	if dec.Kind != operability.Correct || dec.Owner != operability.OwnerDeployer || len(dec.Checks) != 1 || dec.Checks[0].ID != "O1.patches" {
		t.Fatalf("decision = %+v", dec)
	}
}

// A CronJob or bare Pod runs images like any workload: an off-allowlist image
// there fails O6.allowlist and the plan must name the object (O1.workloads).
func TestAllowlistCoversCronJobAndPod(t *testing.T) {
	cron := "apiVersion: batch/v1\nkind: CronJob\nmetadata: {name: nightly}\nspec: {schedule: '0 * * * *', jobTemplate: {spec: {template: {spec: {containers: [{name: c, image: 'evil.example/job:1'}]}}}}}\n"
	pod := "apiVersion: v1\nkind: Pod\nmetadata: {name: shell}\nspec: {containers: [{name: c, image: 'evil.example/shell:1'}]}\n"
	for name, tc := range map[string]struct{ manifest, id string }{"CronJob": {cron, "CronJob/app/nightly"}, "Pod": {pod, "Pod/app/shell"}} {
		t.Run(name, func(t *testing.T) {
			d := offline.Deployment{
				Render:            renderer(t, rendertest.MapFS{"k8s/web.yaml": web, "k8s/extra.yaml": tc.manifest}),
				ExpectedWorkloads: []string{webID, tc.id},
				Policy:            offline.Policy{AllowedRegistries: []string{"registry.example"}},
			}
			c := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
			want(t, c, "O1.workloads", operability.StatusPass, "")
			got := want(t, c, "O6.allowlist", operability.StatusFail, operability.CauseAllowlist)
			if !strings.Contains(got.Detail, "evil.example/") {
				t.Fatalf("detail = %q", got.Detail)
			}
			d.ExpectedWorkloads = []string{webID}
			if c := byID(t, offline.CheckDeployment(context.Background(), d).Checks); c["O1.workloads"].Status != operability.StatusFail {
				t.Fatalf("plan without %s accepted: %+v", tc.id, c["O1.workloads"])
			}
		})
	}
}

func TestUnservedAPIFailsO1APIs(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.ServedAPIs = []string{"v1", "apps/v1", "apps/v1/Deployment"}
	got := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.apis", operability.StatusPass, "")
	d.ServedAPIs = []string{"v1", "apps/v1"}
	got = byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.apis", operability.StatusFail, operability.CauseRender)
}

// With no recorded discovery O1.apis is skipped, not absent, so the report
// always names the check; a render failure lists it among the checks not run.
func TestO1APIsIsAlwaysReported(t *testing.T) {
	d := deployment(t, rendertest.MapFS{"k8s/web.yaml": web})
	d.ServedAPIs = nil
	got := byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.apis", operability.StatusSkipped, "")
	d.Render = func(context.Context) (rendered.DesiredSet, error) { return rendered.DesiredSet{}, errors.New("boom") }
	got = byID(t, offline.CheckDeployment(context.Background(), d).Checks)
	want(t, got, "O1.apis", operability.StatusSkipped, "")
}
