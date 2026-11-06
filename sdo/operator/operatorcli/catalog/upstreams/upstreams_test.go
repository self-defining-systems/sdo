package upstreams_test

import (
	"reflect"
	"testing"

	"sdo.dev/operator/operatorcli/catalog/catalogtest"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
)

// implementation maps a watcher ID to its package under upstream/. The ID
// ("git", the kind in sources.yaml) names what is followed while the package
// names how (gitpoll).
var implementation = map[string]string{"git": "gitpoll"}

func TestCatalogRegistersBuiltIns(t *testing.T) {
	if got, want := upstreams.Catalog().IDs(), []string{"git"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

// A watcher cannot be added to Catalog without running the shared conformance
// suite in its own package and being listed in the architecture component map.
func TestEveryRegisteredIDHasConformanceFixtureAndDocsRow(t *testing.T) {
	catalogtest.Completeness(t, catalogtest.Spec{
		IDs:         upstreams.Catalog().IDs(),
		Packages:    implementation,
		FamilyDir:   "../../../upstream",
		DocsPath:    "sdo/operator/upstream/",
		DocsFile:    "../../../../../docs/architecture.md",
		SupportDirs: []string{"upstreamtest"},
		Suite:       "upstreamtest.TestWatcher",
	})
}
