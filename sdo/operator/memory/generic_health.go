package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/yaml.v3"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// PrepareHealth is the explicit Go bootstrap step shared by memory check and
// broker acceptance. It installs policy detectors from final workload files;
// it never translates or repairs the judge's authored artifacts.
func PrepareHealth(root string) error {
	if _, err := os.Stat(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health-objective"))); os.IsNotExist(err) {
		return fmt.Errorf("health judge omitted health-objective detector")
	} else if err != nil {
		return err
	}
	endpoint := genericManifest("service-endpoints", "15s", []layout.Watch{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Endpoints"}, {APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"}})
	if err := writeGenericDetector(root, endpoint, endpointSource()); err != nil {
		return err
	}
	if err := writeGenericDetector(root, workloadManifest(), workloadSource()); err != nil {
		return err
	}
	directory := filepath.Join(root, layout.TrafficWorkloadsDir)
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		var fields map[string]any
		if err := yaml.Unmarshal(data, &fields); err != nil {
			return err
		}
		data, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		workload := &contracts.TrafficWorkload{}
		if err := protojson.Unmarshal(data, workload); err != nil {
			return fmt.Errorf("traffic workload %s: %w", entry.Name(), err)
		}
		if workload.GetName() != strings.TrimSuffix(entry.Name(), ".yaml") || !layout.ValidDetectorID(workload.GetName()) {
			return fmt.Errorf("traffic workload %s must use its logical filename ID", entry.Name())
		}
		if workload.GetPurpose() != "health-probe" && workload.GetPurpose() != "link-probe" {
			continue
		}
		id := "traffic-" + workload.GetName()
		manifest := genericManifest(id, "10s", []layout.Watch{{APIVersion: "sdo.dev/v1alpha1", Kind: "SyntheticTraffic"}})
		if err := writeGenericDetector(root, manifest, trafficSource(id, workload.GetName(), workload.GetPurpose() == "link-probe")); err != nil {
			return err
		}
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
	formatted, err := format.Source([]byte(source))
	if err != nil {
		return err
	}
	formattedTest, err := format.Source([]byte(test))
	if err != nil {
		return err
	}
	files := []layout.File{{Name: layout.DetectorFileName, Content: formatted}, {Name: layout.DetectorTestFileName, Content: formattedTest}}
	encoded, err := layout.MarshalDetectorManifest(manifest)
	if err != nil {
		return err
	}
	files = append(files, layout.File{Name: layout.ManifestFileName, Content: encoded})
	// An existing policy package must match exactly. Never hide a malformed
	// authored file by overwriting it before validation.
	base := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, manifest.ID))
	entries, err := os.ReadDir(base)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != layout.ManifestFileName && entry.Name() != layout.DetectorFileName && entry.Name() != layout.DetectorTestFileName {
			return fmt.Errorf("generic health package %s contains unexpected file %s", manifest.ID, entry.Name())
		}
	}
	for _, file := range files {
		path := filepath.Join(base, file.Name)
		data, err := os.ReadFile(path)
		if err == nil && !bytes.Equal(data, file.Content) {
			return fmt.Errorf("generic health file %s differs from Go policy; remove the edit", layout.DetectorDir(layout.ClassHealth, manifest.ID)+"/"+file.Name)
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return layout.WriteDetector(root, layout.Detector{Class: layout.ClassHealth, Manifest: manifest, Files: files[:2]})
}

func genericPackage(id string) string {
	if id == "service-endpoints" {
		return "serviceendpoints"
	}
	if id == workloadHealthID {
		return "workloads"
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

const workloadHealthID = "workload-health"

// workloadManifest registers sdk/workloadhealth; its watches must equal
// workloadhealth.Watches().
func workloadManifest() layout.DetectorManifest {
	return genericManifest(workloadHealthID, "15s", []layout.Watch{{APIVersion: "v1", Kind: "Pod"}, {APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "apps/v1", Kind: "ReplicaSet"}, {APIVersion: "v1", Kind: "Event"}})
}

func workloadSource() string {
	return fmt.Sprintf(`package workloads

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/workloadhealth"
)

func New() sdk.Detector {
	return workloadhealth.NewDetector(sdk.DetectorSpec{
		%s
		Watches: workloadhealth.Watches(),
		Interval: 15 * time.Second,
	})
}
`, genericSpec(workloadHealthID))
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
