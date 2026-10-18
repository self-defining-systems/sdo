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
)

func generateRegistrationTest(root string, manifest manifest) error {
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
	for index, item := range manifest.Detectors {
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
    if err != nil { t.Fatalf("healthy baseline: %v", err) }
    for _, detector := range All() {
        if detector.Spec().Class != sdk.DetectorClassIncident { continue }
        for _, violation := range sdktest.HealthyBaselineViolations(detector, snapshots) {
            t.Error(violation)
        }
    }
}
`
