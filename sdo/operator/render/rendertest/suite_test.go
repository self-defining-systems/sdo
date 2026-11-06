package rendertest_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
)

func toyCatalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	if err := rendertest.RegisterToy(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func cm(name string) string {
	return fmt.Sprintf(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":%q}}`, name)
}

const secretJSON = `{"apiVersion":"v1","kind":"Secret","metadata":{"name":"creds"},"x-secret-file":"secrets/token"}`

func toyCases() []rendertest.Case {
	cfg := wrapperspb.String("app")
	return []rendertest.Case{
		{
			Name:   "configmaps default the namespace",
			FS:     rendertest.MapFS{"app/a.json": cm("a"), "app/b.json": cm("b"), "app/README": "ignored"},
			Env:    render.Env{Namespace: "demo"},
			Config: cfg,
			Want:   []string{"ConfigMap demo/a", "ConfigMap demo/b"},
		},
		{
			Name: "secret identity is stable and omit skips its source",
			FS: rendertest.MapFS{
				"app/s.json": secretJSON, "app/a.json": cm("a"), "secrets/token": "hunter2",
			},
			Env:        render.Env{Namespace: "demo"},
			Config:     cfg,
			Want:       []string{"ConfigMap demo/a", "Secret demo/creds"},
			SecretOnly: []string{"secrets/token"},
		},
		{
			Name: "hook phase is not desired state",
			FS: rendertest.MapFS{
				"app/a.json": cm("a"), "app/hook-migrate.json": cm("migrate"),
			},
			Env:    render.Env{Namespace: "demo"},
			Config: cfg,
			Want:   []string{"ConfigMap demo/a"},
		},
		{
			Name:    "missing tracked input",
			FS:      rendertest.MapFS{"app/s.json": secretJSON},
			Env:     render.Env{Namespace: "demo", Secrets: render.SecretsInclude},
			Config:  cfg,
			WantErr: "secrets/token",
		},
		{
			Name:      "input over the file limit",
			FS:        rendertest.MapFS{"app/a.json": cm("a") + "                                                  "},
			Config:    cfg,
			Limits:    &render.Limits{FileBytes: 40},
			WantErr:   "app/a.json",
			WantLimit: true,
		},
		{
			Name:    "missing directory",
			FS:      rendertest.MapFS{},
			Config:  cfg,
			WantErr: "app",
		},
	}
}

func TestToyRendererConforms(t *testing.T) {
	rendertest.TestRenderer(t, toyCatalog(t), rendertest.ToyID, toyCases())
}

// failing registers r and returns the suite's failures over a case set that
// the toy renderer passes.
func failing(t *testing.T, r render.Renderer[*wrapperspb.StringValue]) []rendertest.Failure {
	t.Helper()
	c := render.NewCatalog()
	if err := render.Register[*wrapperspb.StringValue](c, "control", r); err != nil {
		t.Fatal(err)
	}
	return rendertest.Check(c, "control", []rendertest.Case{{
		Name:       "control",
		FS:         rendertest.MapFS{"app/a.json": cm("a"), "app/s.json": secretJSON, "secrets/token": "x"},
		Env:        render.Env{Namespace: "demo"},
		Config:     wrapperspb.String("app"),
		SecretOnly: []string{"secrets/token"},
	}})
}

func properties(fs []rendertest.Failure) map[string]bool {
	out := map[string]bool{}
	for _, f := range fs {
		out[f.Property] = true
	}
	return out
}

func toyObject(kind, name, ns string) *unstructured.Unstructured {
	o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"name": name}}}
	o.SetNamespace(ns)
	return o
}

// Negative controls: each renderer breaks one clause of the contract and the
// suite must say so. A suite that passes any of these is not checking it.

func TestNegativeControlNondeterministicRenderer(t *testing.T) {
	var calls atomic.Int64
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, _ render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		return render.SinglePhase(toyObject("ConfigMap", fmt.Sprintf("cm-%d", calls.Add(1)), "demo")), nil
	}))
	if !properties(got)["determinism"] {
		t.Fatalf("suite accepted a nondeterministic renderer: %v", got)
	}
}

func TestNegativeControlSecretLeakingRenderer(t *testing.T) {
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, fsys render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		value, err := fsys.ReadFile("secrets/token")
		if err != nil {
			return render.Result{}, err
		}
		secret := toyObject("Secret", "creds", "demo")
		secret.Object["stringData"] = map[string]any{"token": string(value)} // ignores env.Secrets
		return render.SinglePhase(secret), nil
	}))
	if !properties(got)["secret-omit"] {
		t.Fatalf("suite accepted a Secret-leaking renderer: %v", got)
	}
}

func TestNegativeControlOmitReadsSecretSource(t *testing.T) {
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, fsys render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		if _, err := fsys.ReadFile("secrets/token"); err != nil { // read, then drop the value
			return render.Result{}, err
		}
		return render.SinglePhase(toyObject("Secret", "creds", "demo")), nil
	}))
	if !properties(got)["secret-omit"] {
		t.Fatalf("suite accepted a renderer that reads Secret sources under SecretsOmit: %v", got)
	}
}

func TestNegativeControlSecretIdentityDependsOnMode(t *testing.T) {
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, _ render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		name := "creds"
		if env.Secrets == render.SecretsInclude {
			name = "creds-hash1234" // a content-hash suffix only computable with values
		}
		return render.SinglePhase(toyObject("Secret", name, "demo")), nil
	}))
	if !properties(got)["secret-identity"] {
		t.Fatalf("suite accepted mode-dependent Secret identity: %v", got)
	}
}

func TestNegativeControlOmitDoesNotDeclareSecretSource(t *testing.T) {
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, fsys render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		if env.Secrets == render.SecretsInclude {
			if _, err := fsys.ReadFile("secrets/token"); err != nil {
				return render.Result{}, err
			}
		}
		return render.SinglePhase(toyObject("Secret", "creds", "demo")), nil
	}))
	if !properties(got)["inputs-modes"] {
		t.Fatalf("suite accepted a renderer whose Omit inputs omit the Secret source: %v", got)
	}
}

func TestNegativeControlRepeatedPhaseNamesRejected(t *testing.T) {
	got := failing(t, render.RendererFunc[*wrapperspb.StringValue](func(_ context.Context, _ render.FS, env render.Env, _ *wrapperspb.StringValue) (render.Result, error) {
		a := render.Phase{Name: "p", Objects: []*unstructured.Unstructured{toyObject("ConfigMap", "a", "demo")}}
		return render.Result{Phases: []render.Phase{a, a}}, nil
	}))
	if len(got) == 0 {
		t.Fatal("suite accepted repeated phase names")
	}
}
