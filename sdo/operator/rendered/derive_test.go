package rendered_test

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
)

const deployment = "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: api\n"

func TestDeriveSourceFromTheFilesPresent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fsys     rendertest.MapFS
		renderer string
		config   proto.Message
	}{
		{"one chart, vendored subcharts are part of it", rendertest.MapFS{
			"chart/Chart.yaml": "name: app\n", "chart/values.yaml": "",
			"chart/charts/db/Chart.yaml": "name: db\n", "src/main.go": "package main\n",
		}, "helm", &contracts.HelmRenderConfig{ChartPath: "chart"}},
		{"overlay over a base: the overlay is the root", rendertest.MapFS{
			"deploy/base/kustomization.yaml":          "resources:\n- app.yaml\n",
			"deploy/base/app.yaml":                    deployment,
			"deploy/overlays/kind/kustomization.yaml": "resources:\n- ../../base\n",
			"deploy/overlays/kind/patch.yaml":         "x: 1\n",
		}, "kustomize", &contracts.KustomizeRenderConfig{Path: "deploy/overlays/kind"}},
		{"a kustomization at the repository root", rendertest.MapFS{
			"kustomization.yaml": "resources:\n- app.yaml\n", "app.yaml": deployment,
		}, "kustomize", &contracts.KustomizeRenderConfig{Path: "."}},
		{"plain manifests: every Kubernetes object file, nothing else", rendertest.MapFS{
			"k8s/api.yaml": deployment, "k8s/svc.yml": "apiVersion: v1\nkind: Service\nmetadata:\n  name: api\n---\n",
			"config/app.yaml": "port: 80\n", ".github/workflows/ci.yaml": deployment,
			"skaffold.yaml": "apiVersion: skaffold/v4beta6\nkind: Config\n", ".sdo/deployment/manifests/x.yaml": deployment,
		}, "raw", &contracts.RawRenderConfig{Paths: []string{"k8s/api.yaml", "k8s/svc.yml"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rendered.DeriveSource(tc.fsys, deriveCatalog(t))
			if err != nil {
				t.Fatal(err)
			}
			if got.GetRendererId() != tc.renderer {
				t.Fatalf("renderer = %q, want %q", got.GetRendererId(), tc.renderer)
			}
			config, err := got.GetConfig().UnmarshalNew()
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(config, tc.config) {
				t.Fatalf("config = %v, want %v", config, tc.config)
			}
		})
	}
}

func TestDeriveSourceRefusesWhatTheFilesCannotSay(t *testing.T) {
	for _, tc := range []struct {
		name string
		fsys rendertest.MapFS
		kind rendered.SourceErrorKind
		want []string
	}{
		{"no deployment files", rendertest.MapFS{"main.go": "package main\n", "config.yaml": "a: 1\n"}, rendered.SourceUnconfigured,
			[]string{"no deployment files"}},
		{"two charts", rendertest.MapFS{"a/Chart.yaml": "name: a\n", "b/Chart.yaml": "name: b\n"}, rendered.SourceInvalid,
			[]string{"a (helm)", "b (helm)", ".sdo/deployment/plan.yaml"}},
		{"a chart beside a kustomization", rendertest.MapFS{
			"helm/Chart.yaml": "name: a\n", "k8s/kustomization.yaml": "resources:\n- app.yaml\n", "k8s/app.yaml": deployment,
		}, rendered.SourceInvalid, []string{"helm (helm)", "k8s (kustomize)"}},
		{"two overlays", rendertest.MapFS{
			"base/kustomization.yaml": "resources:\n- app.yaml\n", "base/app.yaml": deployment,
			"dev/kustomization.yaml": "resources:\n- ../base\n", "prod/kustomization.yaml": "bases:\n- ../base\n",
		}, rendered.SourceInvalid, []string{"dev (kustomize)", "prod (kustomize)", ".sdo/deployment/plan.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := rendered.DeriveSource(tc.fsys, deriveCatalog(t))
			var source *rendered.SourceError
			if !errors.As(err, &source) || source.Kind != tc.kind {
				t.Fatalf("err = %v, want a %s source error", err, tc.kind)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("err = %v, want it to name %q", err, want)
				}
			}
		})
	}
}

func TestResolveSourcePrefersTheOverride(t *testing.T) {
	override := helmSource(t, "b")
	got, err := rendered.ResolveSource(rendertest.MapFS{"a/Chart.yaml": "name: a\n", "b/Chart.yaml": "name: b\n"}, deriveCatalog(t), override)
	if err != nil {
		t.Fatal(err)
	}
	if got != override {
		t.Fatalf("source = %v, want the override", got)
	}
}

func helmSource(t *testing.T, chart string) *contracts.DeploymentSource {
	t.Helper()
	return mustSource(t, "helm", &contracts.HelmRenderConfig{ChartPath: chart})
}

func mustSource(t *testing.T, renderer string, config proto.Message) *contracts.DeploymentSource {
	t.Helper()
	packed, err := anypb.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return &contracts.DeploymentSource{RendererId: renderer, Config: packed}
}

func deriveCatalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := catalog(t)
	if err := helm.Register(c, helm.InProcess); err != nil {
		t.Fatal(err)
	}
	return c
}
