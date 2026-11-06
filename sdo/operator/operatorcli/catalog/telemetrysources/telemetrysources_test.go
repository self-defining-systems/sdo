package telemetrysources_test

import (
	"reflect"
	"testing"

	"sdo.dev/operator/operatorcli/catalog/catalogtest"
	"sdo.dev/operator/operatorcli/catalog/telemetrysources"
)

// implementation maps an adapter name to its package under telemetry/.
var implementation = map[string]string{"prometheus": "prometheus"}

func TestRegistryHoldsBuiltIns(t *testing.T) {
	if got, want := telemetrysources.Registry().Adapters(), []string{"prometheus"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("adapters = %v, want %v", got, want)
	}
}

// An adapter cannot be added to Registry without running the shared
// conformance suite in its own package and being listed in the architecture
// component map.
func TestEveryRegisteredAdapterHasConformanceFixtureAndDocsRow(t *testing.T) {
	catalogtest.Completeness(t, catalogtest.Spec{
		IDs:         telemetrysources.Registry().Adapters(),
		Packages:    implementation,
		FamilyDir:   "../../../telemetry",
		DocsPath:    "sdo/operator/telemetry/",
		DocsFile:    "../../../../../docs/architecture.md",
		SupportDirs: []string{"telemetrytest"},
		Suite:       "telemetrytest.TestClient",
	})
}
