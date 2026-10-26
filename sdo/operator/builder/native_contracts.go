package builder

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
)

func generateRegistrationTest(root string, detectors []layout.Detector) error {
	linkProbes := map[string]bool{}
	workloads, err := loadTrafficWorkloads(filepath.Join(root, "traffic", "workloads"))
	if err != nil {
		return err
	}
	for name, document := range workloads {
		if strings.Contains(string(document), `"purpose":"link-probe"`) {
			linkProbes["traffic-"+name] = true
		}
	}
	var source strings.Builder
	source.WriteString(`package generated

import (
    "reflect"
    "testing"
    "time"
    "sdo.dev/operator/sdk"
)

func TestRegistrationContracts(t *testing.T) {
    detectors := All()
`)
	for index, detector := range detectors {
		item := detector.Manifest
		interval, _ := time.ParseDuration(item.Interval)
		debounce, _ := time.ParseDuration(item.Batching.Debounce)
		watches := []string{}
		synthetic := false
		for _, watch := range item.Watches {
			watches = append(watches, fmt.Sprintf("{APIVersion:%q, Kind:%q, Namespace:%q}", watch.APIVersion, watch.Kind, watch.Namespace))
			if watch.APIVersion == "sdo.dev/v1alpha1" && watch.Kind == "SyntheticTraffic" {
				synthetic = true
			}
		}
		playbooks := []string{}
		for _, playbook := range item.PossiblePlaybooks {
			playbooks = append(playbooks, strconv.Quote(playbook))
		}
		minDuration := ""
		if item.Class == "health" && synthetic && !linkProbes[item.ID] {
			minDuration = ", MinDuration: 9000000000"
		}
		fmt.Fprintf(&source, `    {
        got := detectors[%d].Spec()
        if got.ID != %q || string(got.Class) != %q || string(got.Owner) != %q || got.Interval != time.Duration(%d) ||
            got.Persistence != (sdk.PersistencePolicy{Firing:%d, Clearing:%d%s}) ||
            got.Batching != (sdk.BatchingPolicy{Severity:sdk.FindingSeverity(%q), Debounce:time.Duration(%d)}) ||
            got.OriginatingIncident != %q || got.OriginatingCommit != %q ||
            !reflect.DeepEqual(got.Watches, []sdk.WatchKind{%s}) ||
            !reflect.DeepEqual(got.Playbooks, []string{%s}) {
            t.Errorf("detector registration mismatch for %%q: %%#v", %q, got)
        }
    }
`, index, item.ID, item.Class, item.Owner, interval.Nanoseconds(), item.Persistence.Firing, item.Persistence.Clearing, minDuration,
			item.Batching.Severity, debounce.Nanoseconds(), item.OriginatingIncident, item.OriginatingCommit,
			strings.Join(watches, ","), strings.Join(playbooks, ","), item.ID)
	}
	source.WriteString("}\n")
	return os.WriteFile(filepath.Join(root, "generated", "registrations_test.go"), []byte(source.String()), 0o644)
}

func generateExternalNameTest(appRoot, root string) error {
	names := map[string]bool{"sdo-externalname-probe": true}
	_ = filepath.WalkDir(appRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry.IsDir() {
			if path != appRoot && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" || entry.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}
		info, err := entry.Info()
		if err != nil || info.Size() > 1024*1024 {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		decoder := yaml.NewDecoder(strings.NewReader(string(data)))
		for {
			var document struct {
				Kind     string `yaml:"kind"`
				Metadata struct {
					Name string `yaml:"name"`
				} `yaml:"metadata"`
			}
			if err := decoder.Decode(&document); err != nil {
				if err != io.EOF {
					return nil
				}
				break
			}
			if document.Kind == "Service" && serviceNamePattern.MatchString(document.Metadata.Name) {
				names[document.Metadata.Name] = true
			}
		}
		return nil
	})
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	var source strings.Builder
	source.WriteString(`package generated

import (
    "testing"
    "sdo.dev/operator/sdk"
    "sdo.dev/operator/sdk/sdktest"
)

func TestHealthDetectorsExemptExternalNameServicesFromEndpointChecks(t *testing.T) {
    services := []string{`)
	for _, name := range sorted {
		source.WriteString(strconv.Quote(name) + ",")
	}
	source.WriteString(`}
    for _, detector := range All() {
        if detector.Spec().Class != sdk.DetectorClassHealth { continue }
        for _, violation := range sdktest.ExternalNameEndpointViolations(detector, services) {
            t.Error(violation)
        }
    }
}
`)
	return os.WriteFile(filepath.Join(root, "generated", "externalname_invariants_test.go"), []byte(source.String()), 0o644)
}

func generateHealthyBaselineTest(baseline, root string) error {
	entries, err := os.ReadDir(baseline)
	if err != nil {
		return fmt.Errorf("healthy baseline: %w", err)
	}
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return fmt.Errorf("healthy baseline must contain plain snapshot files: %s", entry.Name())
		}
	}
	fixtures := filepath.Join(root, "generated", "testdata", "healthy-baseline")
	if err := os.MkdirAll(fixtures, 0o755); err != nil {
		return err
	}
	count := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(baseline, entry.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(fixtures, entry.Name()), data, 0o644); err != nil {
			return err
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("healthy baseline has no *.json snapshot files: %s", baseline)
	}
	return os.WriteFile(filepath.Join(root, "generated", "healthy_baseline_test.go"), []byte(healthyBaselineTest), 0o644)
}

const healthyBaselineTest = `package generated

import (
    "testing"
    "sdo.dev/operator/sdk"
    "sdo.dev/operator/sdk/sdktest"
)

func TestIncidentDetectorsStayQuietOnHealthyBaseline(t *testing.T) {
    snapshots, err := sdktest.LoadSnapshots("testdata/healthy-baseline")
    if err != nil { t.Fatalf("detector.healthy-baseline: cannot evaluate controller healthy capture: %v", err) }
    for _, detector := range All() {
        if detector.Spec().Class != sdk.DetectorClassIncident { continue }
        for _, violation := range sdktest.HealthyBaselineViolations(detector, snapshots) {
            t.Fatal("detector.healthy-baseline: " + violation)
        }
    }
}
`

// controllerStartTest runs the exact validation the controller applies to its
// detectors at start, so memory the broker accepts always loads.
const controllerStartTest = `package generated

import (
    "testing"
    "sdo.dev/operator/core"
)

func TestDetectorsLoadInController(t *testing.T) {
    if err := core.ValidateDetectors(All()); err != nil {
        t.Fatal("detector.controller-start: the controller would refuse to start: " + err.Error())
    }
}
`

func generateControllerStartTest(root string) error {
	return os.WriteFile(filepath.Join(root, "generated", "controller_start_test.go"), []byte(controllerStartTest), 0o644)
}

const metricsContractTest = `package generated

import (
    "testing"
    "sdo.dev/operator/sdk/metrics"
)

func TestMetricsDetectorsDeclareValidQueries(t *testing.T) {
    detectors := All()
    for _, detector := range detectors {
        if err := metrics.ValidateDetector(detector); err != nil {
            t.Errorf("detector %s: %v", detector.Spec().ID, err)
        }
    }
    if _, err := metrics.Queries(detectors); err != nil {
        t.Error(err)
    }
}
`

// generateMetricsTest checks every detector's metric declarations: unique,
// non-empty query IDs and expressions, a bounded query count, and rules the
// threshold constructor accepts.
func generateMetricsTest(root string) error {
	return os.WriteFile(filepath.Join(root, "generated", "metrics_contract_test.go"), []byte(metricsContractTest), 0o644)
}
