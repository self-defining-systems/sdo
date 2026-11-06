package kustomize_test

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/rendertest"
)

func catalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	if err := kustomize.Register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func cfg(path string) *contracts.KustomizeRenderConfig {
	return &contracts.KustomizeRenderConfig{Path: path}
}

const deployment = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec: {containers: [{name: web, image: web:1}]}
`

const secretManifest = `apiVersion: v1
kind: Secret
metadata: {name: raw-creds}
stringData: {token: hunter2}
`

func cases() []rendertest.Case {
	env := render.Env{Namespace: "demo"}
	return []rendertest.Case{
		{
			Name: "root with a local base and generators",
			FS: rendertest.MapFS{
				"app/kustomization.yaml": "namePrefix: p-\nresources: [../base, extra.yaml]\n" +
					"configMapGenerator:\n- {name: cfg, files: [conf/init.sh]}\n" +
					"generatorOptions: {disableNameSuffixHash: true}\n",
				"app/extra.yaml":          "apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: sa}\n",
				"app/conf/init.sh":        "echo hi\n",
				"base/kustomization.yaml": "resources: [deploy.yaml]\n",
				"base/deploy.yaml":        deployment,
			},
			Env:    env,
			Config: cfg("app"),
			Want:   []string{"ConfigMap /p-cfg", "Deployment /p-web", "ServiceAccount /p-sa"},
		},
		{
			Name: "secretGenerator and Secret manifest stay identity-only until Include",
			FS: rendertest.MapFS{
				"k/kustomization.yaml": "resources: [secret.yaml]\nsecretGenerator:\n- {name: gen, files: [private/password], envs: [private/env]}\n" +
					"generatorOptions: {disableNameSuffixHash: true}\n",
				"k/secret.yaml":      secretManifest,
				"k/private/password": "pw",
				"k/private/env":      "A=b\n",
			},
			Env:        env,
			Config:     cfg("k"),
			Want:       []string{"Secret /gen", "Secret /raw-creds"},
			SecretOnly: []string{"k/private/password", "k/private/env"},
		},
		{
			Name:   "root directory",
			FS:     rendertest.MapFS{"kustomization.yaml": "resources: [d.yaml]\nnamespace: other\n", "d.yaml": deployment},
			Env:    env,
			Config: cfg(""),
			Want:   []string{"Deployment other/web"},
		},
		{
			Name:    "no kustomization in directory",
			FS:      rendertest.MapFS{"app/a.yaml": deployment},
			Config:  cfg("app"),
			WantErr: "no kustomization",
		},
		{
			Name:      "input over the file limit",
			FS:        rendertest.MapFS{"k/kustomization.yaml": "resources: [d.yaml]\n", "k/d.yaml": deployment},
			Config:    cfg("k"),
			Limits:    &render.Limits{FileBytes: 64},
			WantErr:   "k/d.yaml",
			WantLimit: true,
		},
	}
}

func TestConforms(t *testing.T) {
	rendertest.TestRenderer(t, catalog(t), kustomize.ID, cases())
}

func renderErr(t *testing.T, files rendertest.MapFS, mode render.SecretMode, path string) error {
	t.Helper()
	c := catalog(t)
	a, err := anypb.New(cfg(path))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Render(context.Background(), files, render.Env{Namespace: "demo", Secrets: mode}, kustomize.ID, a)
	return err
}

// N3: a Secret-only input that is not tracked is declared, not read, under
// SecretsOmit, so reads render; the trusted deploy fails with a typed error.
func TestMissingSecretInputPassesInOmitAndFailsInInclude(t *testing.T) {
	files := rendertest.MapFS{"k/kustomization.yaml": "secretGenerator:\n- {name: gen, files: [private/password]}\ngeneratorOptions: {disableNameSuffixHash: true}\n"}
	if err := renderErr(t, files, render.SecretsOmit, "k"); err != nil {
		t.Fatalf("Omit: %v", err)
	}
	err := renderErr(t, files, render.SecretsInclude, "k")
	if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), "k/private/password") {
		t.Fatalf("Include error = %v, want fs.ErrNotExist naming k/private/password", err)
	}
}

func TestOmitDeclaresSecretInputWithItsBlob(t *testing.T) {
	files := rendertest.MapFS{
		"k/kustomization.yaml": "secretGenerator:\n- {name: gen, files: [pw=private/password]}\ngeneratorOptions: {disableNameSuffixHash: true}\n",
		"k/private/password":   "pw",
	}
	a, _ := anypb.New(cfg("k"))
	res, err := catalog(t).Render(context.Background(), files, render.Env{Namespace: "demo"}, kustomize.ID, a)
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range res.Inputs {
		if in.Path == "k/private/password" {
			if in.Kind != render.InputStat || in.Blob != render.GitBlobID([]byte("pw")) {
				t.Fatalf("input = %+v, want stat with the blob id", in)
			}
			return
		}
	}
	t.Fatalf("inputs %+v lack the Secret source", res.Inputs)
}

func TestIncludeCarriesSecretData(t *testing.T) {
	files := rendertest.MapFS{
		"k/kustomization.yaml": "secretGenerator:\n- {name: gen, files: [password]}\ngeneratorOptions: {disableNameSuffixHash: true}\n",
		"k/password":           "pw",
	}
	a, _ := anypb.New(cfg("k"))
	res, err := catalog(t).Render(context.Background(), files, render.Env{Namespace: "demo", Secrets: render.SecretsInclude}, kustomize.ID, a)
	if err != nil {
		t.Fatal(err)
	}
	objs := res.Objects()
	if len(objs) != 1 || objs[0].Object["data"] == nil {
		t.Fatalf("objects = %v, want one Secret with data", objs)
	}
}

func TestRejects(t *testing.T) {
	tests := []struct {
		name  string
		files rendertest.MapFS
		want  string
	}{
		{"hash-suffixed Secret generator", rendertest.MapFS{"k/kustomization.yaml": "secretGenerator:\n- {name: gen, literals: [a=b]}\n"}, "disableNameSuffixHash"},
		{"remote base", rendertest.MapFS{"k/kustomization.yaml": "resources: [github.com/x/y//z]\n"}, "github.com/x/y"},
		{"remote url base", rendertest.MapFS{"k/kustomization.yaml": "resources: [\"https://example.com/x.yaml\"]\n"}, "local"},
		{"exec generator plugin", rendertest.MapFS{"k/kustomization.yaml": "generators: [gen.yaml]\n", "k/gen.yaml": "apiVersion: x/v1\nkind: Exec\nmetadata: {name: g}\n"}, "unsupported kustomization field generators"},
		{"KRM transformer", rendertest.MapFS{"k/kustomization.yaml": "transformers: [t.yaml]\n", "k/t.yaml": "kind: T\n"}, "unsupported kustomization field transformers"},
		{"helm charts", rendertest.MapFS{"k/kustomization.yaml": "helmCharts:\n- {name: c, repo: \"https://example.com\"}\n"}, "unsupported kustomization field helmCharts"},
		{"patches", rendertest.MapFS{"k/kustomization.yaml": "patches: []\n"}, "unsupported kustomization field patches"},
		{"escaping resource", rendertest.MapFS{"k/kustomization.yaml": "resources: [../../x]\n"}, "escapes"},
		{"escaping generator input", rendertest.MapFS{"k/kustomization.yaml": "configMapGenerator:\n- {name: c, files: [../../etc/passwd]}\n"}, "escapes"},
		{"unsupported generator field", rendertest.MapFS{"k/kustomization.yaml": "configMapGenerator:\n- {name: c, plugin: x}\n"}, "unsupported ConfigMap generator field plugin"},
		{"kustomization file as resource", rendertest.MapFS{"k/kustomization.yaml": "resources: [other/kustomization.yaml]\n", "k/other/kustomization.yaml": "resources: []\n"}, "name its directory"},
	}
	for _, tc := range tests {
		for _, mode := range []render.SecretMode{render.SecretsOmit, render.SecretsInclude} {
			t.Run(tc.name, func(t *testing.T) {
				err := renderErr(t, tc.files, mode, "k")
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("mode %d: error = %v, want %q", mode, err, tc.want)
				}
			})
		}
	}
}

func TestRejectsRootEscape(t *testing.T) {
	if err := renderErr(t, rendertest.MapFS{}, render.SecretsOmit, "../x"); err == nil {
		t.Fatal("want an error for a root outside the tree")
	}
}
