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
	if err := rendered.CheckExpectedWorkloads(set, []string{"Deployment/app/web", "Job/app/job"}); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string][]string{
		"missing": {"Deployment/app/web"},
		"extra":   {"Deployment/app/web", "Job/app/job", "Job/app/ghost"},
		"dup":     {"Deployment/app/web", "Deployment/app/web"},
		"empty":   {"", "Job/app/job"},
	} {
		if err := rendered.CheckExpectedWorkloads(set, expected); err == nil {
			t.Errorf("%s: expected workloads accepted", name)
		}
	}
}

// A generated name suffix (a Job named after a hash of its images) is not part
// of the workload's identity: the plan declares `prefix-*` and every release's
// render matches it, while a literal entry still names exactly one workload.
func TestExpectedWorkloadsMatchGeneratedNamesByStableIdentity(t *testing.T) {
	render := func(job string) rendered.DesiredSet {
		manifest := "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec: {template: {spec: {containers: [{name: c, image: 'r/web:1'}]}}}\n" +
			"---\napiVersion: batch/v1\nkind: Job\nmetadata: {name: " + job + "}\nspec: {template: {spec: {containers: [{name: c, image: 'r/job:1'}]}}}\n"
		set, err := rendered.Desired(context.Background(), rendertest.MapFS{"k8s/all.yaml": manifest}, render.Env{Namespace: "app"}, catalog(t), rawSource(t, "k8s"))
		if err != nil {
			t.Fatal(err)
		}
		return set
	}
	expected := []string{"Deployment/app/web", "Job/app/migrate-*"}
	for _, job := range []string{"migrate-d63dd88", "migrate-0a1b2c3d4e", "migrate-abcde"} {
		if err := rendered.CheckExpectedWorkloads(render(job), expected); err != nil {
			t.Errorf("job %s: %v", job, err)
		}
	}
	for name, tc := range map[string]struct {
		job      string
		expected []string
	}{
		"a different stable name":      {"seed-d63dd88", expected},
		"no generated token":           {"migrate", expected},
		"a nested suffix":              {"migrate-d63dd88-x", expected},
		"a pattern that matches none":  {"migrate-d63dd88", []string{"Deployment/app/web", "Job/app/migrate-*", "Job/app/other-*"}},
		"a literal for a generated":    {"migrate-d63dd88", []string{"Deployment/app/web", "Job/app/migrate-aaaaaaa"}},
		"a wildcard in the namespace":  {"migrate-d63dd88", []string{"Deployment/app/web", "Job/*/migrate-*"}},
		"a bare wildcard":              {"migrate-d63dd88", []string{"Deployment/app/web", "Job/app/*"}},
		"a mid-name wildcard":          {"migrate-d63dd88", []string{"Deployment/app/web", "Job/app/mig*-d63dd88"}},
		"the same pattern twice":       {"migrate-d63dd88", []string{"Deployment/app/web", "Job/app/migrate-*", "Job/app/migrate-*"}},
		"a pattern for the Deployment": {"migrate-d63dd88", []string{"Deployment/app/web-*", "Job/app/migrate-*"}},
	} {
		if err := rendered.CheckExpectedWorkloads(render(tc.job), tc.expected); err == nil {
			t.Errorf("%s: accepted", name)
		}
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
