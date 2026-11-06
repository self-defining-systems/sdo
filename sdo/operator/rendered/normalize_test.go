package rendered

import (
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
)

func workloadDoc() map[string]any {
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "c", "image": "img:1"}},
		}}},
	}
}

func TestNormalizeDefaultsNamespaceWithoutStampingOrImages(t *testing.T) {
	doc := workloadDoc()
	ref, workload, err := Normalize(doc, "prod")
	if err != nil || !workload {
		t.Fatalf("Normalize = %v, %v", workload, err)
	}
	want := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "prod", Name: "web"}
	if ref != want {
		t.Fatalf("ref = %+v, want %+v", ref, want)
	}
	meta := doc["metadata"].(map[string]any)
	if _, ok := meta["annotations"]; ok {
		t.Fatalf("Normalize must not stamp: %v", meta)
	}
	image := doc["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"]
	if image != "img:1" {
		t.Fatalf("image changed: %v", image)
	}
}

func TestNormalizeClusterScopedAndNonWorkload(t *testing.T) {
	doc := map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "x", "namespace": "stale"}}
	ref, workload, err := Normalize(doc, "prod")
	if err != nil || workload || ref.Namespace != "" {
		t.Fatalf("got %+v %v %v", ref, workload, err)
	}
	if _, ok := doc["metadata"].(map[string]any)["namespace"]; ok {
		t.Fatal("cluster-scoped namespace not cleared")
	}
}

func TestNormalizeRejectsMalformed(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(map[string]any)
		want   string
	}{
		"no name": {func(d map[string]any) { d["metadata"] = map[string]any{} }, "metadata.name"},
		"no kind": {func(d map[string]any) { delete(d, "kind") }, "kind and metadata"},
		"no spec": {func(d map[string]any) { delete(d, "spec") }, "needs spec"},
		"no images": {func(d map[string]any) {
			d["spec"].(map[string]any)["template"].(map[string]any)["spec"] = map[string]any{}
		}, "no immutable container images"},
	} {
		doc := workloadDoc()
		tc.mutate(doc)
		if _, _, err := Normalize(doc, "prod"); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}
