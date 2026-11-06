package rendered_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

// toyTree loads apps/toy/kubernetes under "raw/" and the Helm variant chart
// under "chart/".
func toyTree(t *testing.T) rendertest.MapFS {
	t.Helper()
	toy := filepath.Join("..", "..", "..", "apps", "toy")
	tree := rendertest.MapFS{}
	for prefix, dir := range map[string]string{"raw": "kubernetes", "chart": "variants/helm/chart"} {
		root := filepath.Join(toy, filepath.FromSlash(dir))
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := os.ReadFile(p)
			rel, _ := filepath.Rel(root, p)
			tree[prefix+"/"+filepath.ToSlash(rel)] = string(data)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return tree
}

// normalizedSet renders through the catalog and normalizes like core does,
// dropping object-level labels and annotations that renderers add.
func normalizedSet(t *testing.T, tree rendertest.MapFS, id string, cfg *anypb.Any) map[string]string {
	t.Helper()
	c := render.NewCatalog()
	for _, register := range []func(*render.Catalog) error{raw.Register, helm.Register} {
		if err := register(c); err != nil {
			t.Fatal(err)
		}
	}
	res, err := c.Render(context.Background(), tree, render.Env{Namespace: "toy", Release: "toy", Secrets: render.SecretsOmit}, id, cfg)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, obj := range res.Desired() {
		ref, _, err := rendered.Normalize(obj.Object, "toy")
		if err != nil {
			t.Fatal(err)
		}
		meta := obj.Object["metadata"].(map[string]any)
		delete(meta, "labels")
		delete(meta, "annotations")
		key := ref.Kind + " " + ref.Namespace + "/" + ref.Name
		if _, dup := out[key]; dup {
			t.Fatalf("duplicate object %s", key)
		}
		// Compare as JSON: renderers may decode numbers to different Go types.
		data, err := json.Marshal(obj.Object)
		if err != nil {
			t.Fatal(err)
		}
		out[key] = string(data)
	}
	return out
}

func toyEquivalence(t *testing.T, tree rendertest.MapFS) (helmSet, rawSet map[string]string) {
	t.Helper()
	helmCfg, err := anypb.New(&v1alpha1.HelmRenderConfig{ChartPath: "chart"})
	if err != nil {
		t.Fatal(err)
	}
	rawCfg, err := anypb.New(&v1alpha1.RawRenderConfig{Paths: []string{"raw"}})
	if err != nil {
		t.Fatal(err)
	}
	return normalizedSet(t, tree, helm.ID, helmCfg), normalizedSet(t, tree, raw.ID, rawCfg)
}

func TestToyHelmChartRendersTheRawManifests(t *testing.T) {
	helmSet, rawSet := toyEquivalence(t, toyTree(t))
	if len(rawSet) != 6 {
		t.Fatalf("raw toy renders %d objects, want 3 Deployments and 3 Services", len(rawSet))
	}
	if !reflect.DeepEqual(helmSet, rawSet) {
		t.Fatalf("helm and raw toy differ\nhelm only: %v\nraw only: %v\nchanged: %v", missing(helmSet, rawSet), missing(rawSet, helmSet), changed(helmSet, rawSet))
	}
}

func TestToyEquivalenceDetectsAChartMutation(t *testing.T) {
	tree := toyTree(t)
	const template = "chart/templates/toy.yaml"
	before := tree[template]
	mutated := strings.Replace(before, "terminationGracePeriodSeconds: 0", "terminationGracePeriodSeconds: 30", 1)
	if mutated == before {
		t.Fatal("mutation did not apply; the chart template changed shape")
	}
	tree[template] = mutated
	helmSet, rawSet := toyEquivalence(t, tree)
	if reflect.DeepEqual(helmSet, rawSet) {
		t.Fatal("a mutated chart template still equals the raw manifests")
	}
}

func missing(a, b map[string]string) []string {
	var out []string
	for k := range a {
		if _, ok := b[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func changed(a, b map[string]string) []string {
	var out []string
	for k, v := range a {
		if w, ok := b[k]; ok && !reflect.DeepEqual(v, w) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
