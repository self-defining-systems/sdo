//archcheck:ratchet R5 count=1 until=S3-10d
package rendered_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/kustomize"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

// The kustomize renderer in SecretsOmit mode must produce the objects that
// rendered.Source (today's reads) derives from the same tree. S3-10d replaces
// Source's generator code with the catalog and turns this into its test.
func TestKustomizeRendererMatchesSourceReads(t *testing.T) {
	const deployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec: {containers: [{name: web, image: web:1}]}\n"
	fixtures := map[string]rendertest.MapFS{
		"overlay with base, prefix and generators": {
			"app/kustomization.yaml": "namePrefix: p-\nresources: [../base, svc.yaml, secret.yaml]\n" +
				"configMapGenerator:\n- {name: cfg, files: [conf/init.sh], literals: [a=b]}\n" +
				"secretGenerator:\n- {name: gen, files: [private/pw], literals: [x=y]}\n" +
				"generatorOptions: {disableNameSuffixHash: true}\n",
			"app/svc.yaml":            "apiVersion: v1\nkind: Service\nmetadata: {name: web}\nspec: {ports: [{port: 80}]}\n",
			"app/secret.yaml":         "apiVersion: v1\nkind: Secret\nmetadata: {name: raw, annotations: {k: v}}\nstringData: {token: hunter2}\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: beside}\n",
			"app/conf/init.sh":        "echo hi\n",
			"app/private/pw":          "pw",
			"base/kustomization.yaml": "resources: [deploy.yaml]\ncommonLabels: {tier: web}\n",
			"base/deploy.yaml":        deployment,
		},
		"namespace and images transformers": {
			"k/kustomization.yaml": "namespace: demo\nresources: [d.yaml]\nimages: [{name: web, newTag: \"2\"}]\nreplicas: [{name: web, count: 3}]\n",
			"k/d.yaml":             deployment,
		},
	}
	for name, files := range fixtures {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "repo")
			if err := os.Mkdir(root, 0o755); err != nil {
				t.Fatal(err)
			}
			testutil.InitGit(t, root)
			for file, body := range files {
				full := filepath.Join(root, file)
				if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			testutil.Git(t, root, "add", ".")
			testutil.Git(t, root, "commit", "-qm", "synthetic")
			rev := strings.TrimSpace(testutil.Git(t, root, "rev-parse", "HEAD"))
			dir := "app"
			if _, ok := files["k/kustomization.yaml"]; ok {
				dir = "k"
			}

			want, err := (rendered.Source{Repository: root, ManifestPaths: []string{dir + "/kustomization.yaml"}}).RawManifests(context.Background(), rev, "demo")
			if err != nil {
				t.Fatal(err)
			}
			c := render.NewCatalog()
			if err := kustomize.Register(c); err != nil {
				t.Fatal(err)
			}
			cfg, _ := anypb.New(&contracts.KustomizeRenderConfig{Path: dir})
			res, err := c.Render(context.Background(), files, render.Env{Namespace: "demo"}, kustomize.ID, cfg)
			if err != nil {
				t.Fatal(err)
			}
			got := res.Desired()
			if len(got) != len(want) {
				t.Fatalf("renderer produced %d objects, Source %d", len(got), len(want))
			}
			for i := range want {
				// Core, not the renderer, defaults the namespace.
				if got[i].GetNamespace() == "" && !rendered.ClusterScopedKind(got[i].GetKind()) {
					got[i].SetNamespace("demo")
				}
				a, _ := json.Marshal(got[i].Object)
				b, _ := json.Marshal(want[i].Object)
				if string(a) != string(b) {
					t.Errorf("object %d differs\nrenderer: %s\nsource:   %s", i, a, b)
				}
			}
		})
	}
}
