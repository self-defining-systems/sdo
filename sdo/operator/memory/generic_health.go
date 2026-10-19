package memory

import (
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// The health judge authors the objective predicate. These generic registrations
// are operator policy, installed at the physical memory edge alongside it.
func installGenericHealth(root string, detectors map[string]layout.Detector, workloads []*contracts.TrafficWorkload) error {
	if _, ok := detectors["health-objective"]; !ok {
		return fmt.Errorf("health judge omitted health-objective detector")
	}
	endpoint := genericManifest("service-endpoints", "15s", []layout.Watch{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Endpoints"}, {APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"}})
	if err := writeGenericDetector(root, endpoint, endpointSource()); err != nil {
		return err
	}
	detectors[endpoint.ID] = layout.Detector{Class: layout.ClassHealth, Manifest: endpoint}
	for _, workload := range workloads {
		if workload.GetPurpose() != "health-probe" && workload.GetPurpose() != "link-probe" {
			continue
		}
		id := "traffic-" + workload.GetName()
		entry := genericManifest(id, "10s", []layout.Watch{{APIVersion: "sdo.dev/v1alpha1", Kind: "SyntheticTraffic"}})
		if err := writeGenericDetector(root, entry, trafficSource(id, workload.GetName(), workload.GetPurpose() == "link-probe")); err != nil {
			return err
		}
		detectors[id] = layout.Detector{Class: layout.ClassHealth, Manifest: entry}
	}
	return nil
}

func genericManifest(id, interval string, watches []layout.Watch) layout.DetectorManifest {
	return layout.DetectorManifest{
		APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion,
		ID: id, Constructor: "New", Class: string(layout.ClassHealth), Owner: layout.ClassHealth.Owner(),
		Watches: watches, Interval: interval, Persistence: layout.Persistence{Firing: 2, Clearing: 2}, Batching: layout.Batching{Severity: "critical", Debounce: "500ms"},
		PossiblePlaybooks: []string{"health-objective"}, OriginatingCommit: "lifecycle-bootstrap",
	}
}

func writeGenericDetector(root string, manifest layout.DetectorManifest, source string) error {
	test := fmt.Sprintf("package %s\n\nimport \"testing\"\n\nfunc TestRegistration(t *testing.T) { if New().Spec().ID != %q { t.Fatal(\"unexpected detector ID\") } }\n", genericPackage(manifest.ID), manifest.ID)
	return layout.WriteDetector(root, layout.Detector{Class: layout.ClassHealth, Manifest: manifest, Files: []layout.File{
		{Name: layout.DetectorFileName, Content: []byte(source)},
		{Name: layout.DetectorTestFileName, Content: []byte(test)},
	}})
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
