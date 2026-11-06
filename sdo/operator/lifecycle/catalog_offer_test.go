package lifecycle_test

import (
	"context"
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
)

func ids[T any](items []T, id func(T) string) []string {
	out := []string{}
	for _, item := range items {
		out = append(out, id(item))
	}
	return out
}

// The agent is offered exactly what the deployer can deploy: the request
// carries the deployer's own catalogs, described, and nothing else.
func TestDeployerRequestCarriesTheDeployersCatalogs(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	renderers := render.NewCatalog()
	if err := rendertest.RegisterToy(renderers); err != nil {
		t.Fatal(err)
	}
	sources := imagesrc.NewCatalog()
	if err := imagesrc.Register(sources, "fake", imagesrc.Source[*emptypb.Empty](&imagesrctest.Fake{}), imagesrc.Options{}, imagesrc.WithDoc("Fake source.")); err != nil {
		t.Fatal(err)
	}
	h.coordinator.Renderers, h.coordinator.ImageSources = renderers, sources
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	request := h.workers.deployer[0]
	if got := ids(request.GetRenderers(), (*contracts.PluginDescriptor).GetId); !reflect.DeepEqual(got, renderers.IDs()) {
		t.Fatalf("renderers offered = %v, want %v", got, renderers.IDs())
	}
	want := renderers.Describe()[0]
	got := request.GetRenderers()[0]
	if got.GetConfigType() != want.ConfigType || got.GetConfigSchemaJson() != want.ConfigSchemaJSON || got.GetDoc() != want.Doc {
		t.Fatalf("renderer descriptor = %v, want %+v", got, want)
	}
	if got := request.GetImageSources(); len(got) != 1 || got[0].GetId() != "fake" || got[0].GetDoc() != "Fake source." {
		t.Fatalf("image sources offered = %v", got)
	}
}

func TestDeployerRequestOffersNothingWithoutCatalogs(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	if r := h.workers.deployer[0]; len(r.GetRenderers()) != 0 || len(r.GetImageSources()) != 0 {
		t.Fatalf("offered %v %v", r.GetRenderers(), r.GetImageSources())
	}
}
