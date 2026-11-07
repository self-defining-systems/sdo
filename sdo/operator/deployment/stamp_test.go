package deployment

import (
	"reflect"
	"testing"
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
	images, err := Stamp(doc, "tree1", func(s ImageSite) string {
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
	meta := doc["metadata"].(map[string]any)["labels"].(map[string]any)
	tmplMeta, _ := doc["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)
	tmpl, _ := tmplMeta["annotations"].(map[string]any)
	if meta["sdo.dev/source-tree"] != "tree1" {
		t.Fatalf("object not stamped: %v", meta)
	}
	if _, stamped := tmpl["sdo.dev/source-tree"]; stamped {
		t.Fatalf("tree-level stamp in the pod template would re-roll every workload on every advance: %v", tmpl)
	}
}

// The pod template is the workload's own deploy input: stamping two different
// effective trees onto the same workload must leave the template identical, so
// an advance rolls only the workloads whose own spec or image pins changed.
func TestStampTemplateIsIndependentOfTree(t *testing.T) {
	a, b := stampDoc("Deployment"), stampDoc("Deployment")
	pin := func(s ImageSite) string { return s.Image + "@sha" }
	if _, err := Stamp(a, "tree1", pin); err != nil {
		t.Fatal(err)
	}
	if _, err := Stamp(b, "tree2", pin); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a["spec"], b["spec"]) {
		t.Fatalf("spec differs across trees: %v vs %v", a["spec"], b["spec"])
	}
}

func TestStampNonWorkloadOnlyAnnotates(t *testing.T) {
	doc := stampDoc("ConfigMap")
	images, err := Stamp(doc, "tree1", nil)
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

// An advance that changes one workload's image pin re-rolls only that
// workload: its sibling's stamped template is unchanged across the two trees.
func TestStampAdvanceRollsOnlyTheWorkloadWhoseImageChanged(t *testing.T) {
	stamped := func(tree, appImage string) (app, datastore map[string]any) {
		app, datastore = stampDoc("Deployment"), stampDoc("StatefulSet")
		datastore["spec"] = stampDoc("Deployment")["spec"]
		for doc, image := range map[*map[string]any]string{&app: appImage, &datastore: "pg@sha1"} {
			if _, err := Stamp(*doc, tree, func(ImageSite) string { return image }); err != nil {
				t.Fatal(err)
			}
		}
		return app, datastore
	}
	appA, dbA := stamped("tree1", "app@sha1")
	appB, dbB := stamped("tree2", "app@sha2")
	if reflect.DeepEqual(appA["spec"], appB["spec"]) {
		t.Fatal("the workload whose image changed must roll")
	}
	if !reflect.DeepEqual(dbA["spec"], dbB["spec"]) {
		t.Fatalf("untouched workload's template changed: %v vs %v", dbA["spec"], dbB["spec"])
	}
}

// Kubernetes bumps a Deployment's metadata.generation for any change to its
// annotations, so an advance that changes nothing in a Deployment's rendered
// spec must leave its annotations identical: the stamp lives in labels.
func TestStampLeavesDeploymentAnnotationsIndependentOfTree(t *testing.T) {
	a, b := stampDoc("Deployment"), stampDoc("Deployment")
	if _, err := Stamp(a, "tree1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Stamp(b, "tree2", nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a["metadata"].(map[string]any)["annotations"], b["metadata"].(map[string]any)["annotations"]) {
		t.Fatalf("annotations depend on the tree: %v vs %v", a["metadata"], b["metadata"])
	}
}
