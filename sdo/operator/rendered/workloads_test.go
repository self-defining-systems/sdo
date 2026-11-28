package rendered_test

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

func TestImageSitesListInitThenRegularPerWorkload(t *testing.T) {
	manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: c, image: 'r/main:1'}, {name: s, image: 'r/side:1'}], initContainers: [{name: i, image: 'r/init:1'}]}}}\n" +
		"---\napiVersion: v1\nkind: Service\nmetadata: {name: web}\n" +
		"---\napiVersion: batch/v1\nkind: Job\nmetadata: {name: job}\nspec: {template: {spec: {containers: [{name: c, image: 'r/job:1'}]}}}\n"
	set, err := rendered.Desired(context.Background(), rendertest.MapFS{"k8s/all.yaml": manifest}, render.Env{Namespace: "app"}, catalog(t), rawSource(t, "k8s"))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(rendered.ImageSites(set), " ")
	if got != "r/init:1 r/main:1 r/side:1 r/job:1" {
		t.Fatalf("image sites = %q", got)
	}
}

func TestSummarizeWorkloadsProposesStableIdentities(t *testing.T) {
	objects := func(names ...string) []*unstructured.Unstructured {
		var out []*unstructured.Unstructured
		for _, name := range names {
			kind, name, _ := strings.Cut(name, "/")
			out = append(out, &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "batch/v1", "kind": kind, "metadata": map[string]any{"name": name},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "c", "image": "reg.example/app/" + name + ":v1"}}}}},
			}})
		}
		return out
	}
	inventory := rendered.SummarizeWorkloads(objects("Job/migrate-d63dd88", "Job/seed", "Job/data-import", "Deployment/web-5f7d9", "Job/job1"), "app")
	got := map[string]string{}
	for _, w := range inventory.Workloads {
		got[w.ID] = w.Identity
	}
	want := map[string]string{
		"Job/app/migrate-d63dd88":  "Job/app/migrate-*",
		"Job/app/seed":             "Job/app/seed",
		"Job/app/data-import":      "Job/app/data-import",
		"Deployment/app/web-5f7d9": "Deployment/app/web-5f7d9",
		"Job/app/job1":             "Job/app/job1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identities = %v, want %v", got, want)
	}
	if want := []string{"Deployment/app/web-5f7d9", "Job/app/data-import", "Job/app/job1", "Job/app/migrate-*", "Job/app/seed"}; !slices.Equal(inventory.Identities, want) {
		t.Fatalf("identities list = %v, want %v", inventory.Identities, want)
	}
	if want := []string{"reg.example/app/data-import", "reg.example/app/job1", "reg.example/app/migrate-d63dd88", "reg.example/app/seed", "reg.example/app/web-5f7d9"}; !slices.Equal(inventory.Repositories, want) {
		t.Fatalf("repositories = %v, want %v", inventory.Repositories, want)
	}

}
