package raw_test

import (
	"google.golang.org/protobuf/types/known/anypb"

	"testing"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
)

const deployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec: {replicas: 1}
`

const service = `apiVersion: v1
kind: Service
metadata: {name: web, namespace: other}
`

const secret = `apiVersion: v1
kind: Secret
metadata: {name: creds}
stringData: {password: hunter2}
data: {token: eA==}
`

func catalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	if err := raw.Register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func cfg(paths ...string) *v1alpha1.RawRenderConfig { return &v1alpha1.RawRenderConfig{Paths: paths} }

func cases() []rendertest.Case {
	tree := rendertest.MapFS{
		"k8s/web.yaml":            deployment,
		"k8s/sub/svc.yml":         service,
		"k8s/sub/notes.txt":       "not a manifest",
		"k8s/creds.yaml":          secret,
		"k8s/multi.yaml":          deployment + "---\n" + service + "---\nkind: Nameless\n",
		"k8s/ext/a.json":          `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"a"}}`,
		"loose.yaml":              deployment,
		"kust/kustomization.yaml": "resources: [../k8s/web.yaml]\n",
		"kust/Kustomization":      "resources: []\n",
		"bad/broken.yaml":         "kind: [unterminated\n",
		"big/huge.yaml":           deployment + "# " + string(make([]byte, 200)) + "\n",
	}
	env := render.Env{Namespace: "app"}
	return []rendertest.Case{
		{Name: "directory recursive", FS: tree, Env: env, Config: cfg("k8s/sub", "k8s/ext"), Want: []string{"ConfigMap /a", "Service other/web"}},
		{Name: "file and directory", FS: tree, Env: env, Config: cfg("loose.yaml", "k8s/ext"), Want: []string{"ConfigMap /a", "Deployment /web"}},
		{Name: "multi document skips nameless", FS: tree, Env: env, Config: cfg("k8s/multi.yaml"), Want: []string{"Deployment /web", "Service other/web"}},
		{Name: "secret", FS: tree, Env: env, Config: cfg("k8s/creds.yaml"), Want: []string{"Secret /creds"}},
		{Name: "no paths", FS: tree, Env: env, Config: cfg(), WantErr: "no manifest paths"},
		{Name: "escaping path", FS: tree, Env: env, Config: cfg("../x"), WantErr: "invalid path"},
		{Name: "unclean path", FS: tree, Env: env, Config: cfg("k8s/"), WantErr: "invalid path"},
		{Name: "missing path", FS: tree, Env: env, Config: cfg("nope"), WantErr: "has no manifests"},
		{Name: "no manifests in directory", FS: tree, Env: env, Config: cfg("k8s/sub/notes.txt"), WantErr: "has no manifests"},
		{Name: "selected twice", FS: tree, Env: env, Config: cfg("k8s/sub", "k8s/sub/svc.yml"), WantErr: "selected twice"},
		{Name: "kustomize root file", FS: tree, Env: env, Config: cfg("kust/kustomization.yaml"), WantErr: "use renderer kustomize"},
		{Name: "kustomize root directory", FS: tree, Env: env, Config: cfg("kust"), WantErr: "use renderer kustomize"},
		{Name: "decode failure", FS: tree, Env: env, Config: cfg("bad"), WantErr: "decode manifest bad/broken.yaml failed"},
		{Name: "file limit", FS: tree, Env: env, Config: cfg("big"), WantErr: "limit", WantLimit: true, Limits: &render.Limits{FileBytes: 100, TotalBytes: 1 << 20}},
	}
}

func TestRawRendererConforms(t *testing.T) {
	rendertest.TestRenderer(t, catalog(t), raw.ID, cases())
}

func TestSecretsOmitProjectsIdentityOnly(t *testing.T) {
	c := catalog(t)
	config, _ := anypb.New(cfg("k8s/creds.yaml"))
	tree := rendertest.MapFS{"k8s/creds.yaml": secret}
	omit, err := c.Render(t.Context(), tree, render.Env{Namespace: "app", Secrets: render.SecretsOmit}, raw.ID, config)
	if err != nil {
		t.Fatal(err)
	}
	if got := omit.Objects()[0].Object; got["data"] != nil || got["stringData"] != nil {
		t.Fatalf("omit kept Secret data: %v", got)
	}
	include, err := c.Render(t.Context(), tree, render.Env{Namespace: "app", Secrets: render.SecretsInclude}, raw.ID, config)
	if err != nil {
		t.Fatal(err)
	}
	if got := include.Objects()[0].Object; got["stringData"] == nil || got["data"] == nil {
		t.Fatalf("include dropped Secret data: %v", got)
	}
}
