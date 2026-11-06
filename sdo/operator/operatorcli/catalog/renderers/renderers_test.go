package renderers_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/operatorcli/catalog/catalogtest"
	"sdo.dev/operator/operatorcli/catalog/renderers"
)

// implementation maps a renderer ID to its package under render/.
var implementation = map[string]string{"helm": "helm", "kustomize": "kustomize", "raw": "raw"}

func TestCatalogRegistersBuiltIns(t *testing.T) {
	if got, want := renderers.Catalog().IDs(), []string{"helm", "kustomize", "raw"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

// A renderer cannot be added to Catalog without running the shared
// conformance suite in its own package and being listed in the architecture
// component map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	catalogtest.Completeness(t, catalogtest.Spec{
		IDs:         renderers.Catalog().IDs(),
		Packages:    implementation,
		FamilyDir:   "../../../render",
		DocsPath:    "sdo/operator/render/",
		DocsFile:    "../../../../../docs/architecture.md",
		SupportDirs: []string{"rendertest"},
		Suite:       "rendertest.TestRenderer",
	})
}

// TestEveryRegisteredRendererIsDescribed keeps the agent-facing catalog
// complete: a renderer cannot be added without a usage doc, and its config
// schema must be valid JSON.
func TestEveryRegisteredRendererIsDescribed(t *testing.T) {
	descs := renderers.Catalog().Describe()
	if len(descs) != len(renderers.Catalog().IDs()) {
		t.Fatalf("Describe returned %d entries for %d renderers", len(descs), len(renderers.Catalog().IDs()))
	}
	for _, d := range descs {
		t.Run(d.ID, func(t *testing.T) {
			if strings.TrimSpace(d.Doc) == "" {
				t.Error("renderer has no Doc; export Doc from its package and pass render.WithDoc(Doc)")
			}
			if d.ConfigType == "" || !json.Valid([]byte(d.ConfigSchemaJSON)) {
				t.Errorf("descriptor is incomplete: %+v", d)
			}
		})
	}
}
