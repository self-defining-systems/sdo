package planfile

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/prebuilt"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
)

const goodPlan = `source:
  rendererId: raw
  config:
    "@type": type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig
    paths: [deploy/sdo/manifests]
images:
  - image: registry.invalid/app:1
    sourceId: prebuilt
    config:
      "@type": type.googleapis.com/sdodev.contracts.v1alpha1.PrebuiltImageConfig
`

func catalogs(t *testing.T) Catalogs {
	t.Helper()
	r := render.NewCatalog()
	if err := raw.Register(r); err != nil {
		t.Fatal(err)
	}
	i := imagesrc.NewCatalog()
	if err := imagesrc.Register[*contracts.PrebuiltImageConfig](i, prebuilt.ID, &prebuilt.Source{}, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	return Catalogs{Renderers: r, Images: i}
}

func TestParseDecodesThroughBothCatalogs(t *testing.T) {
	plan, err := Parse([]byte(goodPlan), catalogs(t))
	if err != nil {
		t.Fatal(err)
	}
	if plan.GetSource().GetRendererId() != "raw" || len(plan.GetImages()) != 1 {
		t.Fatalf("plan = %v", plan)
	}
	// The plan round-trips: Format then Parse is the identity.
	data, err := Format(plan)
	if err != nil {
		t.Fatal(err)
	}
	again, err := Parse(data, catalogs(t))
	if err != nil || !proto.Equal(plan, again) {
		t.Fatalf("round trip: %v %v", again, err)
	}
	second, _ := Format(again)
	if string(second) != string(data) {
		t.Fatal("Format is not canonical")
	}
}

func TestParseRejects(t *testing.T) {
	for _, tc := range []struct{ name, plan, want string }{
		{"empty", "", "empty"},
		{"unknown field", strings.Replace(goodPlan, "images:", "colour: red\nimages:", 1), "colour"},
		{"duplicate key", goodPlan + "source: {}\n", "duplicate YAML key"},
		{"unknown renderer", strings.Replace(goodPlan, "rendererId: raw", "rendererId: jsonnet", 1), "unknown renderer"},
		{"wrong config type", strings.Replace(strings.Replace(goodPlan, "RawRenderConfig", "KustomizeRenderConfig", 1), "paths: [deploy/sdo/manifests]", "path: deploy/sdo/manifests", 1), "takes config"},
		{"unknown config field", strings.Replace(goodPlan, "paths:", "pathz:", 1), "pathz"},
		{"unknown image source", strings.Replace(goodPlan, "sourceId: prebuilt", "sourceId: ftp", 1), "unknown source"},
		{"retired strategy", "strategyId: raw\n" + goodPlan, "strategyId"},
		{"retired expected workloads", goodPlan + "expectedWorkloadIds: [Deployment/app/api]\n", "expectedWorkloadIds"},
		{"not a mapping", "- a\n- b\n", "mapping"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.plan), catalogs(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// The override may leave out the source: the files then say how to render.
func TestParseAcceptsAnOverrideWithoutASource(t *testing.T) {
	plan, err := Parse([]byte("images: []\n"), catalogs(t))
	if err != nil || plan.GetSource() != nil {
		t.Fatalf("plan = %v, err = %v", plan, err)
	}
}

func TestParseWithoutCatalogsStillDecodesStrictly(t *testing.T) {
	if _, err := Parse([]byte(goodPlan), Catalogs{}); err != nil {
		t.Fatal(err)
	}
	if _, err := Parse([]byte(strings.Replace(goodPlan, "paths:", "pathz:", 1)), Catalogs{}); err == nil {
		t.Fatal("unknown config field accepted without catalogs")
	}
}
