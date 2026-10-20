package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The health judge authors the objective predicate. These generic registrations
// are operator policy, installed at the physical memory edge alongside it.
func installGenericHealth(root string, detectors map[string]artifactDetector, workloads []*contracts.TrafficWorkload) error {
	if _, ok := detectors["health-objective"]; !ok {
		return fmt.Errorf("health judge omitted health-objective detector")
	}
	endpoint := artifactDetector{ID: "service-endpoints", Package: "./detectors/health/service-endpoints", Constructor: "New", Class: "health", Owner: "health_judge", Watches: []artifactWatch{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Endpoints"}, {APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"}}, Interval: "15s", Persistence: artifactPersistence{2, 2}, Batching: artifactBatching{"critical", "500ms"}, PossiblePlaybooks: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap"}
	if err := writeGenericDetector(root, endpoint, endpointSource()); err != nil {
		return err
	}
	detectors[endpoint.ID] = endpoint
	for _, workload := range workloads {
		if workload.GetPurpose() != "health-probe" && workload.GetPurpose() != "link-probe" {
			continue
		}
		id := "traffic-" + workload.GetName()
		entry := artifactDetector{ID: id, Package: "./detectors/health/" + id, Constructor: "New", Class: "health", Owner: "health_judge", Watches: []artifactWatch{{APIVersion: "sdo.dev/v1alpha1", Kind: "SyntheticTraffic"}}, Interval: "10s", Persistence: artifactPersistence{2, 2}, Batching: artifactBatching{"critical", "500ms"}, PossiblePlaybooks: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap"}
		if err := writeGenericDetector(root, entry, trafficSource(id, workload.GetName(), workload.GetPurpose() == "link-probe")); err != nil {
			return err
		}
		detectors[id] = entry
	}
	return nil
}

func writeGenericDetector(root string, entry artifactDetector, source string) error {
	base := filepath.Join(root, ".sdo", "diagnostics", strings.TrimPrefix(entry.Package, "./"))
	if err := os.MkdirAll(base, 0o755); err != nil {
		return err
	}
	if err := writeGoSource(filepath.Join(base, "detector.go"), &contracts.ProposedGoSource{Name: "detector.go", Content: source}); err != nil {
		return err
	}
	test := fmt.Sprintf("package %s\n\nimport \"testing\"\n\nfunc TestRegistration(t *testing.T) { if New().Spec().ID != %q { t.Fatal(\"unexpected detector ID\") } }\n", genericPackage(entry.ID), entry.ID)
	return writeGoSource(filepath.Join(base, "detector_test.go"), &contracts.ProposedGoSource{Name: "detector_test.go", Content: test})
}

func genericPackage(id string) string {
	if id == "service-endpoints" {
		return "serviceendpoints"
	}
	return "synthetic"
}

func genericSpec(id string) string {
	return fmt.Sprintf(`ID: %q,
		Class: sdk.DetectorClassHealth,
		Owner: sdk.DetectorOwnerHealthJudge,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		Playbooks: []string{"health-objective"},
		OriginatingCommit: "lifecycle-bootstrap",`, id)
}

func endpointSource() string {
	return fmt.Sprintf(`package serviceendpoints

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
)

func New() sdk.Detector {
	return servicehealth.NewReadyEndpointsDetector(sdk.DetectorSpec{
		%s
		Watches: servicehealth.EndpointWatches(),
		Interval: 15 * time.Second,
	})
}
`, genericSpec("service-endpoints"))
}

func trafficSource(id, workload string, link bool) string {
	constructor := "NewDetector"
	if link {
		constructor = "NewLinkDetector"
	}
	return fmt.Sprintf(`package synthetic

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

func New() sdk.Detector {
	return traffic.%s(sdk.DetectorSpec{
		%s
		Watches: []sdk.WatchKind{traffic.Watch},
		Interval: 10 * time.Second,
	}, %q)
}
`, constructor, genericSpec(id), workload)
}
