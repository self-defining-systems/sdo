package renderers_test

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
)

const credsManifest = `apiVersion: v1
kind: Secret
metadata:
  name: creds
  namespace: app
  labels: {team: a}
  annotations: {note: x}
type: kubernetes.io/tls
data: {tls.crt: eA==}
stringData: {token: hunter2}
`

// Every renderer projects a Secret to the same identity under SecretsOmit, so
// reads, digests and the deployer's re-render agree whichever renderer a
// source uses. The Include render of the same tree keeps the Secret (type and
// all) and has the same identity.
func TestRenderersProjectSecretsToTheSameIdentity(t *testing.T) {
	identity := map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "creds", "namespace": "app"}}
	cases := []struct {
		renderer string
		fs       rendertest.MapFS
		config   proto.Message
	}{
		{"raw", rendertest.MapFS{"m/creds.yaml": credsManifest}, &contracts.RawRenderConfig{Paths: []string{"m/creds.yaml"}}},
		{"kustomize", rendertest.MapFS{
			"k/kustomization.yaml": "namespace: app\nresources: [creds.yaml]\n",
			"k/creds.yaml":         credsManifest,
		}, &contracts.KustomizeRenderConfig{Path: "k"}},
		{"kustomize generator", rendertest.MapFS{
			"k/kustomization.yaml": "namespace: app\nsecretGenerator:\n- {name: creds, literals: [token=hunter2]}\ngeneratorOptions: {disableNameSuffixHash: true}\n",
		}, &contracts.KustomizeRenderConfig{Path: "k"}},
		{"helm", rendertest.MapFS{
			"c/Chart.yaml":           "apiVersion: v2\nname: c\nversion: 0.1.0\n",
			"c/templates/creds.yaml": credsManifest,
		}, &contracts.HelmRenderConfig{ChartPath: "c"}},
	}
	catalog := renderers.Catalog()
	for _, tc := range cases {
		t.Run(tc.renderer, func(t *testing.T) {
			id := tc.renderer
			if id == "kustomize generator" {
				id = "kustomize"
			}
			config, err := anypb.New(tc.config)
			if err != nil {
				t.Fatal(err)
			}
			env := render.Env{Namespace: "app", Release: "app", Secrets: render.SecretsOmit}
			omit, err := catalog.Render(t.Context(), tc.fs, env, id, config)
			if err != nil {
				t.Fatal(err)
			}
			secrets := 0
			for _, object := range omit.Objects() {
				if object.GetKind() != "Secret" {
					continue
				}
				secrets++
				if !reflect.DeepEqual(object.Object, identity) {
					t.Errorf("omit Secret = %v, want %v", object.Object, identity)
				}
			}
			if secrets != 1 {
				t.Fatalf("omit render has %d Secrets, want 1", secrets)
			}
			env.Secrets = render.SecretsInclude
			include, err := catalog.Render(t.Context(), tc.fs, env, id, config)
			if err != nil {
				t.Fatal(err)
			}
			for _, object := range include.Objects() {
				if object.GetKind() == "Secret" && !reflect.DeepEqual(render.SecretIdentity(object.Object), identity) {
					t.Errorf("include Secret identity = %v, want %v", render.SecretIdentity(object.Object), identity)
				}
			}
		})
	}
}
