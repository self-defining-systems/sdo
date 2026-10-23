package memory

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/workloadhealth"
)

func preparedHealthRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health-objective"))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("id: health-objective\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPrepareHealthInstallsWorkloadHealthDetector(t *testing.T) {
	root := preparedHealthRoot(t)
	data, err := os.ReadFile(filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "workload-health")))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := layout.ParseDetectorManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	var watches []string
	for _, watch := range manifest.Watches {
		watches = append(watches, watch.APIVersion+"/"+watch.Kind)
	}
	var want []string
	for _, watch := range workloadhealth.Watches() {
		want = append(want, watch.APIVersion+"/"+watch.Kind)
	}
	if manifest.ID != "workload-health" || manifest.Class != string(layout.ClassHealth) || manifest.Owner != layout.ClassHealth.Owner() ||
		manifest.Persistence != (layout.Persistence{Firing: 2, Clearing: 2}) || manifest.Batching.Severity != "critical" ||
		!reflect.DeepEqual(watches, want) {
		t.Fatalf("workload-health manifest %+v must match the SDK detector watches %v", manifest, want)
	}
	source, err := os.ReadFile(filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "workload-health"), layout.DetectorFileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"package workloads", "workloadhealth.NewDetector(", "workloadhealth.Watches()", `ID:                "workload-health"`} {
		if !strings.Contains(string(source), fragment) {
			t.Fatalf("generated workload-health source lacks %q:\n%s", fragment, source)
		}
	}
	// Preparing an already prepared tree must accept the identical policy.
	if err := PrepareHealth(root); err != nil {
		t.Fatalf("second preparation: %v", err)
	}
}

func TestPrepareHealthInstallsServiceReachabilityForEveryApplication(t *testing.T) {
	root := t.TempDir()
	objective := filepath.Join(root, layout.DetectorManifestPath(layout.ClassHealth, "health-objective"))
	if err := os.MkdirAll(filepath.Dir(objective), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objective, []byte("judge authored\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// No traffic workloads: the probe needs no authoring at all.
	if err := PrepareHealth(root); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, "service-reachability"))
	data, err := os.ReadFile(filepath.Join(directory, layout.ManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := layout.ParseDetectorManifest(data)
	if err != nil {
		t.Fatal(err)
	}
	watches := make([]string, 0, len(manifest.Watches))
	for _, watch := range manifest.Watches {
		watches = append(watches, watch.APIVersion+"/"+watch.Kind)
	}
	want := make([]string, 0)
	for _, watch := range servicehealth.ReachabilityWatches() {
		want = append(want, watch.APIVersion+"/"+watch.Kind)
	}
	if manifest.ID != "service-reachability" || manifest.Class != string(layout.ClassHealth) || manifest.Interval != "10s" ||
		!reflect.DeepEqual(watches, want) {
		t.Fatalf("manifest: %+v watches %v", manifest, watches)
	}
	source, err := os.ReadFile(filepath.Join(directory, layout.DetectorFileName))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"package servicereachability", "servicehealth.NewReachabilityDetector", "servicehealth.ReachabilityWatches()"} {
		if !strings.Contains(string(source), needle) {
			t.Fatalf("detector source must contain %q:\n%s", needle, source)
		}
	}
	if err := PrepareHealth(root); err != nil {
		t.Fatalf("preparation must be idempotent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, layout.DetectorFileName), append(source, []byte("// edited\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := PrepareHealth(root); err == nil || !strings.Contains(err.Error(), "differs from Go policy") {
		t.Fatalf("an edited generic package must be rejected, got %v", err)
	}
}
