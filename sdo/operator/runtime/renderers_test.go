package runtime

import (
	"testing"

	"sdo.dev/operator/render"
)

// The renderer catalog enters the controller through Run and RuntimeOptions;
// both accept nil (no renderers wired yet).
func TestRendererCatalogEntryPoints(t *testing.T) {
	var run func(*render.Catalog) = Run
	_ = run
	catalog := render.NewCatalog()
	options := RuntimeOptions{Renderers: catalog}
	if options.Renderers != catalog {
		t.Fatal("RuntimeOptions.Renderers did not retain the catalog")
	}
	if (RuntimeOptions{}).Renderers != nil {
		t.Fatal("zero RuntimeOptions must carry no catalog")
	}
}
