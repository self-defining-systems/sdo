package planfile

import (
	"slices"
	"testing"
)

const buildPlan = `source:
  rendererId: raw
  config:
    "@type": type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig
    paths: [k8s]
images:
  - image: registry.invalid/api:1
    sourceId: buildkit
    config:
      "@type": type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig
      contextPath: services/api
      dockerfilePath: services/api/Dockerfile
  - image: registry.invalid/cache:1
    sourceId: prebuilt
    config:
      "@type": type.googleapis.com/sdodev.contracts.v1alpha1.PrebuiltImageConfig
`

func TestUnbuiltPathsAreThoseNoBuildContextOrRendererInputCovers(t *testing.T) {
	plan, err := Parse([]byte(buildPlan), Catalogs{})
	if err != nil {
		t.Fatal(err)
	}
	got := Unbuilt(plan, []string{"services/api/main.go", "services/rate/rate.go", "k8s/cm.yaml", "README"})
	want := []string{"README", "services/rate/rate.go"}
	if !slices.Equal(got, want) {
		t.Fatalf("Unbuilt = %v, want %v", got, want)
	}
}

func TestUnbuiltIsEmptyWhenAContextCoversTheRoot(t *testing.T) {
	plan, err := Parse([]byte(`{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}},"images":[{"image":"a:1","sourceId":"buildkit","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig","contextPath":".","dockerfilePath":"Dockerfile"}}]}`), Catalogs{})
	if err != nil {
		t.Fatal(err)
	}
	if got := Unbuilt(plan, []string{"cmd/x/main.go"}); len(got) != 0 {
		t.Fatalf("Unbuilt = %v, want none", got)
	}
}
