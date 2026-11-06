package rendered_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
)

func TestDeploymentPlanFlagRoundTrips(t *testing.T) {
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "chart", ValueFiles: []string{"values.yaml"}})
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.DeploymentPlan{
		Source:              &contracts.DeploymentSource{RendererId: "helm", Config: config},
		Images:              []*contracts.ImageSource{{Image: "registry.invalid/app:v1"}},
		ExpectedWorkloadIds: []string{"Deployment/app/web"},
	}
	encoded, err := rendered.FormatDeploymentPlan(want)
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(encoded, "\n\t") {
		t.Fatalf("flag value must be one compact line: %q", encoded)
	}
	got, err := rendered.ParseDeploymentPlan(encoded)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("round trip = %v, %v; want %v", got, err, want)
	}
}

func TestDeploymentPlanFlagRejectsWhatIsNotAPlan(t *testing.T) {
	if plan, err := rendered.ParseDeploymentPlan(""); plan != nil || err != nil {
		t.Fatalf("empty = %v, %v, want no plan", plan, err)
	}
	for name, encoded := range map[string]string{
		"not json":      "chart",
		"unknown field": `{"source":{"rendererId":"raw"},"manifests":["k8s"]}`,
		"no source":     `{"expectedWorkloadIds":["Deployment/app/web"]}`,
	} {
		if _, err := rendered.ParseDeploymentPlan(encoded); err == nil || !strings.Contains(err.Error(), "invalid deployment plan") {
			t.Errorf("%s: err = %v, want an invalid deployment plan error", name, err)
		}
	}
}
