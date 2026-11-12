package builder

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
)

func generateRegistrationTest(root string, detectors []layout.Detector) error {
	var source strings.Builder
	source.WriteString("package generated\n\nimport (\n\"testing\"\n")
	if len(detectors) > 0 {
		source.WriteString("\"reflect\"\n\"time\"\n\"sdo.dev/operator/sdk\"\n")
	}
	source.WriteString(")\n\nfunc TestRegistrationContracts(t *testing.T) {\n detectors := All()\n")
	fmt.Fprintf(&source, "if len(detectors) != %d { t.Fatalf(\"registered %%d detectors; expected %d\", len(detectors)) }\n", len(detectors), len(detectors))
	for index, detector := range detectors {
		item := detector.Manifest
		interval, _ := time.ParseDuration(item.Interval)
		debounce, _ := time.ParseDuration(item.Batching.Debounce)
		watches := []string{}
		for _, watch := range item.Watches {
			watches = append(watches, fmt.Sprintf("{APIVersion:%q, Kind:%q, Namespace:%q}", watch.APIVersion, watch.Kind, watch.Namespace))
		}
		playbooks := []string{}
		for _, playbook := range item.PossiblePlaybooks {
			playbooks = append(playbooks, strconv.Quote(playbook))
		}
		fmt.Fprintf(&source, `    {
        got := detectors[%d].Spec()
        if got.ID != %q || string(got.Class) != %q || string(got.Owner) != %q || got.Interval != time.Duration(%d) ||
            got.Persistence != (sdk.PersistencePolicy{Firing:%d, Clearing:%d}) ||
            got.Batching != (sdk.BatchingPolicy{Severity:sdk.FindingSeverity(%q), Debounce:time.Duration(%d)}) ||
            got.OriginatingIncident != %q || got.OriginatingCommit != %q ||
            !reflect.DeepEqual(got.Watches, []sdk.WatchKind{%s}) ||
            !reflect.DeepEqual(got.Playbooks, []string{%s}) {
            t.Errorf("detector registration mismatch for %%q: %%#v", %q, got)
        }
    }
`, index, item.ID, item.Class, item.Owner, interval.Nanoseconds(), item.Persistence.Firing, item.Persistence.Clearing,
			item.Batching.Severity, debounce.Nanoseconds(), item.OriginatingIncident, item.OriginatingCommit,
			strings.Join(watches, ","), strings.Join(playbooks, ","), item.ID)
	}
	source.WriteString("}\n")
	return os.WriteFile(filepath.Join(root, "generated", "registrations_test.go"), []byte(source.String()), 0o644)
}

func generateExternalNameTest(appRoot, root string) error {
	names := map[string]bool{"sdo-externalname-probe": true}
	// Service names come from the accepted architecture's inventory lines,
	// which Go wrote from the rendered desired state; the builder never parses
	// manifest YAML, so Helm and Kustomize sources contribute like raw ones.
	if data, err := os.ReadFile(filepath.Join(appRoot, filepath.FromSlash(layout.ArchitectureFile))); err == nil {
		for name := range appmap.ParseInventory(string(data)).Services {
			if serviceNamePattern.MatchString(name) {
				names[name] = true
			}
		}
	}
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
            t.Error("detector.healthy-baseline[" + detector.Spec().ID + "]: " + violation)
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
    "sdo.dev/operator/sdk/logs"
    "sdo.dev/operator/sdk/podread"
)

func TestMetricsDetectorsDeclareValidQueries(t *testing.T) {
    detectors := All()
    for _, detector := range detectors {
        if err := logs.ValidateDetector(detector); err != nil { t.Error(err) }
        if err := podread.ValidateDetector(detector); err != nil { t.Error(err) }
        if err := metrics.ValidateDetector(detector); err != nil {
            t.Errorf("detector %s: %v", detector.Spec().ID, err)
        }
    }
    if _,err:=logs.Queries(detectors);err!=nil {t.Error(err)}
    if _,err:=podread.Queries(detectors);err!=nil {t.Error(err)}
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

// TelemetryBindings are the effective telemetry sources of the tree under
// check: operator-provided plus the designer's own. Core never parses a query
// dialect, so the one static link between detectors and bindings is the source
// name each query carries.
type TelemetryBindings struct {
	Sources []telemetry.Source
}

const telemetrySourcesFile = "testdata/telemetry-sources.json"

const telemetryBindingTest = `package generated

import (
    "encoding/json"
    "errors"
    "os"
    "testing"
    "sdo.dev/operator/sdk/metrics"
    "sdo.dev/operator/telemetry"
)

func TestDetectorQueriesResolveToBoundSources(t *testing.T) {
    data, err := os.ReadFile("testdata/telemetry-sources.json")
    if err != nil { t.Fatalf("telemetry.unbound: cannot read the effective telemetry sources: %v", err) }
    var sources []telemetry.Source
    if err := json.Unmarshal(data, &sources); err != nil { t.Fatalf("telemetry.unbound: cannot decode the effective telemetry sources: %v", err) }
    bound := make([]telemetry.Bound, len(sources))
    for i, source := range sources { bound[i] = telemetry.Bound{Source: source} }
    resolver, err := telemetry.NewResolver(bound)
    if err != nil { t.Fatalf("telemetry.unbound: the effective telemetry sources are inconsistent: %v", err) }
    for _, detector := range All() {
        id := detector.Spec().ID
        if consumer, ok := detector.(metrics.Consumer); ok {
            for _, query := range consumer.MetricQueries() {
                if _, err := resolver.Resolve(query.Source, telemetry.KindMetrics); err != nil {
                    rule := "telemetry.unbound"
                    if errors.Is(err, telemetry.ErrKind) { rule = "telemetry.kind" }
                    t.Errorf("%s[%s]: query %q: %v", rule, id, query.ID, err)
                }
            }
        }
        if consumer, ok := detector.(metrics.AlertConsumer); ok && consumer.ConsumesAlerts() && len(resolver.AlertSources()) == 0 {
            t.Errorf("telemetry.unbound[%s]: reads alerts but no bound source serves alerts", id)
        }
    }
}
`

// generateTelemetryBindingTest writes the query-to-source binding check and the
// effective sources it resolves against. With no bindings supplied it writes
// nothing.
func generateTelemetryBindingTest(root string, bindings *TelemetryBindings) error {
	if bindings == nil {
		return nil
	}
	sources := bindings.Sources
	if sources == nil {
		sources = []telemetry.Source{}
	}
	data, err := json.Marshal(sources)
	if err != nil {
		return err
	}
	directory := filepath.Join(root, "generated", "testdata")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "generated", telemetrySourcesFile), data, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "generated", "telemetry_binding_test.go"), []byte(telemetryBindingTest), 0o644)
}

// generateTelemetrySources compiles the accepted telemetry sources file into
// the generated package, so the memory host reports exactly the bindings of
// the revision it was built from (empty when the revision has none).
func generateTelemetrySources(diagnostics, root string) error {
	data, err := os.ReadFile(filepath.Join(diagnostics, "telemetry", "sources.yaml"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("read telemetry sources: %w", err)
	}
	source := "package generated\n\n// TelemetrySources is the accepted .sdo/diagnostics/telemetry/sources.yaml.\nconst TelemetrySources = " + strconv.Quote(string(data)) + "\n"
	return os.WriteFile(filepath.Join(root, "generated", "telemetry_sources.go"), []byte(source), 0o644)
}
