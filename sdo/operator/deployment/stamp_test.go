package deployment

import (
	"reflect"
	"testing"

	"sdo.dev/operator/rendered"
)

func stampDoc(kind string) map[string]any {
	doc := map[string]any{"kind": kind, "metadata": map[string]any{"name": "web", "namespace": "prod"}}
	if kind == "Deployment" {
		doc["spec"] = map[string]any{"template": map[string]any{"spec": map[string]any{
			"initContainers": []any{map[string]any{"name": "i", "image": "init:1"}},
			"containers":     []any{map[string]any{"name": "c", "image": "app:1"}},
		}}}
	}
	return doc
}

func TestStampAnnotatesTreeAndSubstitutesImages(t *testing.T) {
	doc := stampDoc("Deployment")
	var sites []ImageSite
	images, err := Stamp(doc, rendered.BaseTree("tree1"), func(s ImageSite) string {
		sites = append(sites, s)
		return s.Image + "@sha"
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"init:1@sha", "app:1@sha"}; !reflect.DeepEqual(images, want) {
		t.Fatalf("images = %v", images)
	}
	if want := []ImageSite{{"Deployment", "web", "i", true, "init:1"}, {"Deployment", "web", "c", false, "app:1"}}; !reflect.DeepEqual(sites, want) {
		t.Fatalf("sites = %v", sites)
	}
	meta := doc["metadata"].(map[string]any)["annotations"].(map[string]any)
	tmpl := doc["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)
	if meta["sdo.dev/source-tree"] != "tree1" || tmpl["sdo.dev/source-tree"] != "tree1" {
		t.Fatalf("tree not stamped: %v %v", meta, tmpl)
	}
}

func TestStampNonWorkloadOnlyAnnotates(t *testing.T) {
	doc := stampDoc("ConfigMap")
	images, err := Stamp(doc, rendered.BaseTree("tree1"), nil)
	if err != nil || images != nil {
		t.Fatalf("images=%v err=%v", images, err)
	}
	if doc["metadata"].(map[string]any)["annotations"].(map[string]any)["sdo.dev/source-tree"] != "tree1" {
		t.Fatal("not annotated")
	}
}

func TestStampWithoutTreeLeavesDocumentUnannotated(t *testing.T) {
	doc := stampDoc("Deployment")
	if _, err := Stamp(doc, "", nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["metadata"].(map[string]any)["annotations"]; ok {
		t.Fatal("annotated without tree")
	}
}
