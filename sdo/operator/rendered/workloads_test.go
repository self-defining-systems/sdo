package rendered_test

import (
	"context"
	"strings"
	"testing"

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
