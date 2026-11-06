//archcheck:ratchet R5 count=1 until=S3-10d
package rendered_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

const parityRevision = "0123456789abcdef0123456789abcdef01234567"

// treeGit serves `ls-tree -r -z` and `show <rev>:<path>` from a MapFS, the two
// Git reads the accepted-source reader makes.
func treeGit(tree rendertest.MapFS) func(context.Context, string, ...string) ([]byte, error) {
	return func(_ context.Context, _ string, args ...string) ([]byte, error) {
		switch {
		case len(args) >= 1 && args[0] == "ls-tree":
			names := make([]string, 0, len(tree))
			for name := range tree {
				names = append(names, name)
			}
			sort.Strings(names)
			var out strings.Builder
			for _, name := range names {
				fmt.Fprintf(&out, "100644 blob 0000\t%s\x00", name)
			}
			return []byte(out.String()), nil
		case len(args) == 2 && args[0] == "show":
			name := strings.TrimPrefix(args[1], parityRevision+":")
			content, ok := tree[name]
			if !ok {
				return nil, fmt.Errorf("no such path %s", name)
			}
			return []byte(content), nil
		}
		return nil, fmt.Errorf("unexpected git %v", args)
	}
}

// legacyView applies what core does after a renderer: namespace defaulting and
// the foreign-namespace filter that the accepted-source reader performs inline.
func legacyView(objects []*unstructured.Unstructured, namespace string) []map[string]any {
	var out []map[string]any
	for _, o := range objects {
		o = o.DeepCopy()
		scoped := !rendered.ClusterScopedKind(o.GetKind())
		if scoped && o.GetNamespace() == "" {
			o.SetNamespace(namespace)
		}
		if scoped && o.GetNamespace() != namespace {
			continue
		}
		if !scoped {
			o.SetNamespace("")
		}
		out = append(out, o.Object)
	}
	return out
}

const parityDeployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {replicas: 2, template: {spec: {containers: [{name: c, image: 'example/web:1'}]}}}\n"

var parityTree = rendertest.MapFS{
	"k8s/web.yaml":      parityDeployment,
	"k8s/sub/svc.yml":   "apiVersion: v1\nkind: Service\nmetadata: {name: web, namespace: app}\nspec: {ports: [{port: 80}]}\n",
	"k8s/sub/other.yml": "apiVersion: v1\nkind: Service\nmetadata: {name: elsewhere, namespace: other}\n",
	"k8s/sub/notes.txt": "ignored",
	"k8s/creds.yaml":    "apiVersion: v1\nkind: Secret\nmetadata: {name: creds}\nstringData: {password: hunter2}\n",
	"k8s/multi.yaml":    "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: a}\ndata: {n: '1'}\n---\nkind: Nameless\n---\napiVersion: v1\nkind: ConfigMap\nmetadata: {name: b}\n",
	"k8s/ns.json":       `{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"app"}}`,
	"loose.yaml":        parityDeployment,
	"bad/broken.yaml":   "kind: [unterminated\n",
	"dup/a.yaml":        parityDeployment,
	"dup/b.yaml":        parityDeployment,
}

// TestRawRendererMatchesAcceptedSourceReads pins that the raw renderer plus
// core's namespace handling selects and decodes exactly what
// rendered.Source.RawManifests does on the same trees, until S3-10d replaces
// the latter.
func TestRawRendererMatchesAcceptedSourceReads(t *testing.T) {
	catalog := render.NewCatalog()
	if err := raw.Register(catalog); err != nil {
		t.Fatal(err)
	}
	for name, paths := range map[string][]string{
		"directory":         {"k8s"},
		"nested directory":  {"k8s/sub"},
		"file and nested":   {"loose.yaml", "k8s/sub"},
		"one file":          {"k8s/multi.yaml"},
		"secret":            {"k8s/creds.yaml"},
		"json":              {"k8s/ns.json"},
		"decode failure":    {"bad"},
		"missing":           {"absent"},
		"selected twice":    {"k8s/sub", "k8s/sub/svc.yml"},
		"escape":            {"../x"},
		"no paths":          nil,
		"no manifests":      {"k8s/sub/notes.txt"},
		"duplicate objects": {"dup"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			source := rendered.Source{Repository: "/synthetic", ManifestPaths: paths, Git: treeGit(parityTree)}
			want, wantErr := source.RawManifests(ctx, parityRevision, "app")
			config, err := anypb.New(&v1alpha1.RawRenderConfig{Paths: paths})
			if err != nil {
				t.Fatal(err)
			}
			result, gotErr := catalog.Render(ctx, parityTree, render.Env{Namespace: "app", Secrets: render.SecretsOmit}, raw.ID, config)
			if name == "duplicate objects" {
				// Deduplication is core's: the renderer returns both and the
				// shared Desired function (S3-10a) rejects the pair.
				if wantErr == nil || gotErr != nil || len(result.Objects()) != 2 {
					t.Fatalf("duplicate objects: source %v, renderer %v (%d objects)", wantErr, gotErr, len(result.Objects()))
				}
				return
			}
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("source error %v, renderer error %v", wantErr, gotErr)
			}
			if wantErr != nil {
				return
			}
			wantObjects := make([]map[string]any, 0, len(want))
			for _, o := range want {
				wantObjects = append(wantObjects, o.Object)
			}
			if got := legacyView(result.Objects(), "app"); !reflect.DeepEqual(got, wantObjects) {
				t.Fatalf("renderer objects differ from accepted-source reads\n got %v\nwant %v", got, wantObjects)
			}
		})
	}
}
