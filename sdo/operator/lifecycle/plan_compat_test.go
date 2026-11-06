// Transitional with plan_compat.go; deleted by S3-13b.
//
//archcheck:ratchet R5 count=1 until=S3-13b
package lifecycle

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func pack(t *testing.T, m proto.Message) *anypb.Any {
	t.Helper()
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func newForm(t *testing.T, renderer string, images ...*contracts.ImageSource) *contracts.DeploymentPlan {
	return &contracts.DeploymentPlan{
		Source:              &contracts.DeploymentSource{RendererId: renderer, Config: pack(t, &contracts.RawRenderConfig{Paths: []string{"k8s", "extra.yaml"}})},
		Images:              images,
		ExpectedWorkloadIds: []string{"apps/v1/Deployment/ns/web"},
	}
}

func TestAdmitPlanTranslatesNewFormToRawManifestStrategy(t *testing.T) {
	plan := newForm(t, "raw",
		&contracts.ImageSource{Image: "web", SourceId: "buildkit", Config: pack(t, &contracts.BuildKitImageConfig{ContextPath: "svc", DockerfilePath: "svc/Dockerfile"})},
		&contracts.ImageSource{Image: "redis", SourceId: "prebuilt"})
	got, err := admitPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStrategyId() != "raw-manifests-v1" {
		t.Fatalf("strategy id %q", got.GetStrategyId())
	}
	want := &contracts.RawManifestStrategy{
		ImageBuilds:         []*contracts.ImageBuild{{Image: "web", ContextPath: "svc", DockerfilePath: "svc/Dockerfile"}},
		ManifestPaths:       []string{"k8s", "extra.yaml"},
		ExpectedWorkloadIds: []string{"apps/v1/Deployment/ns/web"},
	}
	gotStrategy := &contracts.RawManifestStrategy{}
	if err := got.GetStrategyConfig().UnmarshalTo(gotStrategy); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(gotStrategy, want) {
		t.Fatalf("strategy %v, want %v", gotStrategy, want)
	}
}

func TestAdmitPlanPassesLegacyPlanThrough(t *testing.T) {
	legacy := &contracts.DeploymentPlan{StrategyId: "raw", StrategyConfig: pack(t, &contracts.RawManifestStrategy{})}
	got, err := admitPlan(legacy)
	if err != nil || got != legacy {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestAdmitPlanRejections(t *testing.T) {
	both := newForm(t, "raw")
	both.StrategyId = "raw"
	noPaths := newForm(t, "raw")
	noPaths.Source.Config = pack(t, &contracts.RawRenderConfig{})
	wrongConfig := newForm(t, "raw")
	wrongConfig.Source.Config = pack(t, &contracts.KustomizeRenderConfig{Path: "k"})
	cases := map[string]struct {
		plan *contracts.DeploymentPlan
		want string
	}{
		"empty":                   {&contracts.DeploymentPlan{}, "neither source nor strategy"},
		"both":                    {both, "both source and strategy"},
		"kustomize":               {newForm(t, "kustomize"), `renderer "kustomize" is not supported yet: the renderer must be "raw"`},
		"no paths":                {noPaths, "no paths"},
		"wrong config":            {wrongConfig, "decode raw render config"},
		"unknown image":           {newForm(t, "raw", &contracts.ImageSource{Image: "x", SourceId: "ko"}), `source must be "buildkit" or "prebuilt"`},
		"buildkit without config": {newForm(t, "raw", &contracts.ImageSource{Image: "x", SourceId: "buildkit"}), "config is required"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := admitPlan(c.plan)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want containing %q", err, c.want)
			}
			if strings.Contains(err.Error(), "S3-") {
				t.Fatalf("error %q reaches the deployer agent and names an internal plan row", err)
			}
		})
	}
}
