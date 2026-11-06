package images_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/operatorcli/catalog/catalogtest"
	"sdo.dev/operator/operatorcli/catalog/images"
)

func testConfig() images.Config {
	return images.Config{Builder: buildkit.Builder{AllowedRegistryHost: "reg.example"}, BuildBudget: time.Minute}
}

func TestCatalogRegistersBuiltIns(t *testing.T) {
	c, err := images.Catalog(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.IDs(), []string{"buildkit", "prebuilt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
	if c.Default() != "prebuilt" {
		t.Fatalf("default source = %q, want prebuilt", c.Default())
	}
}

func TestCatalogRejectsInvalidConfig(t *testing.T) {
	for name, mutate := range map[string]func(*images.Config){
		"no push host":    func(c *images.Config) { c.Builder.AllowedRegistryHost = "" },
		"path as host":    func(c *images.Config) { c.Builder.AllowedRegistryHost = "reg.example/ns" },
		"negative budget": func(c *images.Config) { c.BuildBudget = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			mutate(&cfg)
			if _, err := images.Catalog(cfg); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// implementation maps an image source ID to its package under imagesrc/.
var implementation = map[string]string{"buildkit": "buildkit", "prebuilt": "prebuilt"}

// A source cannot be added to Catalog without running the shared catalog
// contract in its own package and being listed in the architecture component
// map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	c, err := images.Catalog(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	catalogtest.Completeness(t, catalogtest.Spec{
		IDs:         c.IDs(),
		Packages:    implementation,
		FamilyDir:   "../../../imagesrc",
		DocsPath:    "sdo/operator/imagesrc/",
		DocsFile:    "../../../../../docs/architecture.md",
		SupportDirs: []string{"imagesrctest"},
		Suite:       "imagesrctest.TestCatalogContract",
	})
}

// TestEveryRegisteredSourceIsDescribed keeps the agent-facing catalog
// complete: a source cannot be added without a usage doc, and its config
// schema must be valid JSON.
func TestEveryRegisteredSourceIsDescribed(t *testing.T) {
	c, err := images.Catalog(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	descs := c.Describe()
	if len(descs) != len(c.IDs()) {
		t.Fatalf("Describe returned %d entries for %d sources", len(descs), len(c.IDs()))
	}
	for _, d := range descs {
		t.Run(d.ID, func(t *testing.T) {
			if strings.TrimSpace(d.Doc) == "" {
				t.Error("source has no Doc; export Doc from its package and pass imagesrc.WithDoc(Doc)")
			}
			if d.ConfigType == "" || !json.Valid([]byte(d.ConfigSchemaJSON)) {
				t.Errorf("descriptor is incomplete: %+v", d)
			}
		})
	}
}

func TestPrebuiltOnlyOffersNoBuilder(t *testing.T) {
	c, err := images.PrebuiltOnly(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.IDs(); !reflect.DeepEqual(got, []string{"prebuilt"}) || c.Default() != "prebuilt" {
		t.Fatalf("IDs = %v default %q", got, c.Default())
	}
}
