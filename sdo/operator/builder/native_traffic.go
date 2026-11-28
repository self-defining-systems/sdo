package builder

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/sdk/traffic"
)

// refusedGeneratorImports are the standard-library packages that break a
// probe: they reach the network, files or processes outside the engine (which
// alone sends a probe's requests, applies the safety rules and counts them),
// can end the prober, or draw randomness the engine does not hand it (the
// same iteration must build the same requests). A refused prefix covers its
// subpackages, except allowedUnder. Everything else in the standard library
// is allowed.
var refusedGeneratorImports = []string{"net", "crypto/tls", "crypto/rand", "math/rand", "hash/maphash", "mime/multipart", "os", "syscall", "log", "flag", "io/ioutil", "path/filepath", "plugin", "runtime", "unsafe", "embed", "debug", "go", "testing", "expvar", "C"}

// allowedUnder are subpackages of a refused prefix that only parse or format.
var allowedUnder = map[string]bool{"net/url": true, "net/mail": true, "net/netip": true, "net/textproto": true, "math/rand/v2": true}

// generatorImportProblem is why a generator may not import name, or "".
func generatorImportProblem(name, module string) string {
	if name == "sdo.dev/operator/sdk/traffic" || name == module+"/traffic/generators" || allowedUnder[name] {
		return ""
	}
	first, _, _ := strings.Cut(name, "/")
	if strings.Contains(first, ".") {
		return "only the standard library and the SDK (sdo.dev/operator/sdk/traffic) are available"
	}
	switch name {
	case "math/rand":
		return "import math/rand/v2: Build receives the iteration's *rand.Rand from math/rand/v2; draw every varying value from it"
	case "crypto/rand", "hash/maphash", "mime/multipart":
		return "it draws randomness the engine does not hand the probe; draw from the *rand.Rand Build receives, so the same iteration builds the same requests"
	}
	for _, prefix := range refusedGeneratorImports {
		if name == prefix || strings.HasPrefix(name, prefix+"/") {
			return "a probe sends requests only through the engine and touches no files or processes; build the request with the SDK and let the engine send it"
		}
	}
	return ""
}

var forbiddenTimeCalls = map[string]bool{
	"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true,
	"AfterFunc": true, "Tick": true, "NewTimer": true, "NewTicker": true,
}

// goBuildInput are the non-Go files the Go tool compiles into a package.
var goBuildInput = map[string]bool{".s": true, ".S": true, ".sx": true, ".syso": true, ".c": true, ".cc": true, ".cpp": true, ".cxx": true,
	".h": true, ".hh": true, ".hpp": true, ".hxx": true, ".m": true, ".f": true, ".F": true, ".for": true, ".f90": true, ".swig": true, ".swigcxx": true}

var serviceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

func generateTraffic(root, module string, target time.Duration) (bool, error) {
	base := filepath.Join(root, "traffic")
	files, err := goSourceFiles(filepath.Join(base, "generators"))
	if err != nil {
		return false, err
	}
	workloads, err := loadTrafficWorkloads(filepath.Join(base, "workloads"), target)
	if err != nil {
		return false, err
	}
	if len(files) == 0 {
		if len(workloads) > 0 {
			return false, fmt.Errorf("traffic workloads exist but diagnostics traffic/generators has no Go package")
		}
		if err := os.WriteFile(filepath.Join(root, "generated", "traffic_test.go"), []byte(noTrafficTest), 0o644); err != nil {
			return false, err
		}
		source := "package generated\n\nimport \"sdo.dev/operator/sdk/traffic\"\n\nfunc TrafficCatalog() traffic.Catalog { return nil }\n\nvar TrafficWorkloads = map[string][]byte{}\n"
		if err := os.WriteFile(filepath.Join(root, "generated", "traffic.go"), []byte(source), 0o644); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := validateGeneratorSources(base, module); err != nil {
		return false, err
	}
	incidentFiles, err := goSourceFiles(filepath.Join(base, "generators", "incident"))
	if err != nil {
		return false, err
	}
	imports := "\t\"sdo.dev/operator/sdk/traffic\"\n\tgenerators " + strconv.Quote(module+"/traffic/generators") + "\n"
	catalog := "\tcatalog := append(traffic.Catalog{}, generators.Scenarios()...)\n"
	if len(incidentFiles) > 0 {
		imports += "\tincident " + strconv.Quote(module+"/traffic/generators/incident") + "\n"
		catalog += "\tcatalog = append(catalog, incident.Scenarios()...)\n"
	}
	names := make([]string, 0, len(workloads))
	for name := range workloads {
		names = append(names, name)
	}
	sort.Strings(names)
	items := ""
	for _, name := range names {
		items += "\t" + strconv.Quote(name) + ": []byte(" + strconv.Quote(string(workloads[name])) + "),\n"
	}
	source := "package generated\n\nimport (\n" + imports + ")\n\n" +
		"func TrafficCatalog() traffic.Catalog {\n" + catalog + "\treturn traffic.CompleteCatalog(catalog, nil)\n}\n\n" +
		"var TrafficWorkloads = map[string][]byte{\n" + items + "}\n"
	if err := os.WriteFile(filepath.Join(root, "generated", "traffic.go"), []byte(source), 0o644); err != nil {
		return false, err
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "traffic_test.go"), []byte(trafficTest), 0o644); err != nil {
		return false, err
	}
	return false, nil
}

func goSourceFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	files := []string{}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}

func loadTrafficWorkloads(dir string, target time.Duration) (map[string]json.RawMessage, error) {
	workloads := map[string]json.RawMessage{}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return workloads, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		ext := filepath.Ext(name)
		if entry.IsDir() || ext != ".yaml" && ext != ".yml" {
			return nil, fmt.Errorf("traffic workloads must be <name>.yaml files: %s", name)
		}
		stem := strings.TrimSuffix(name, ext)
		if !serviceNamePattern.MatchString(stem) {
			return nil, fmt.Errorf("traffic workload file name must be a DNS label: %s", name)
		}
		if _, exists := workloads[stem]; exists {
			return nil, fmt.Errorf("duplicate traffic workload name: %s", stem)
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var value map[string]any
		if err := yaml.Unmarshal(data, &value); err != nil || value == nil {
			return nil, fmt.Errorf("traffic workload %s must be a mapping: %v", name, err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("traffic workload %s: %w", name, err)
		}
		if target > 0 && value["purpose"] == string(traffic.PurposeHealthProbe) {
			workload, err := traffic.ParseWorkloadJSONWithDetectionTarget(encoded, target)
			if err != nil {
				return nil, fmt.Errorf("traffic workload %s: %w", name, err)
			}
			encoded, err = json.Marshal(workload)
			if err != nil {
				return nil, fmt.Errorf("traffic workload %s: %w", name, err)
			}
		}
		workloads[stem] = encoded
	}
	return workloads, nil
}

func validateGeneratorSources(base, module string) error {
	return filepath.WalkDir(filepath.Join(base, "generators"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() && goBuildInput[filepath.Ext(path)] {
			return fmt.Errorf("%s: generators are Go source only; the Go tool would also build this file", path)
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parent := filepath.Dir(path)
		if parent != filepath.Join(base, "generators") && parent != filepath.Join(base, "generators", "incident") {
			return fmt.Errorf("%s: generators live in exactly two packages", path)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("invalid traffic generator %s: %w", path, err)
		}
		for _, imported := range file.Imports {
			name, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return fmt.Errorf("%s: invalid import %s", path, imported.Path.Value)
			}
			if problem := generatorImportProblem(name, module); problem != "" {
				return fmt.Errorf("%s: generators may not import %q: %s", path, name, problem)
			}
		}
		file, err = parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return fmt.Errorf("invalid traffic generator %s: %w", path, err)
		}
		var forbidden string
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			receiver, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if receiver.Name == "rand" && selector.Sel.Name != "Rand" && strings.ToUpper(selector.Sel.Name[:1]) == selector.Sel.Name[:1] || receiver.Name == "time" && forbiddenTimeCalls[selector.Sel.Name] {
				forbidden = receiver.Name + "." + selector.Sel.Name
			}
			return true
		})
		switch {
		case forbidden == "time.Now" || forbidden == "time.Since" || forbidden == "time.Until":
			return fmt.Errorf("%s: generators may not call %s: read the iteration's start time from state[traffic.StateNow] (RFC 3339, UTC), so a replay builds the same requests", path, forbidden)
		case strings.HasPrefix(forbidden, "time."):
			return fmt.Errorf("%s: generators may not call %s: it blocks past the iteration deadline; return traffic.NotYet or wrap the read in traffic.Eventually and the engine sends it again", path, forbidden)
		case forbidden != "":
			return fmt.Errorf("%s: generators may not call %s: draw from the rng Build receives (for example rng.IntN), so the same iteration builds the same requests", path, forbidden)
		}
		return nil
	})
}

const noTrafficTest = `package generated

import (
    "testing"
    "sdo.dev/operator/sdk/traffic"
)

func TestTrafficDetectorsHaveGenerators(t *testing.T) {
    for _, detector := range All() {
        if consumer, ok := detector.(traffic.Consumer); ok && len(consumer.TrafficWorkloads()) > 0 {
            t.Errorf("detector %s consumes traffic workloads but the application has no generators", detector.Spec().ID)
        }
    }
}
`

const trafficTest = `package generated

import (
    "context"
    "encoding/json"
    "os"
    "testing"
    "sdo.dev/operator/runtime/prober"
    "sdo.dev/operator/sdk/traffic"
)

// TestTrafficNotesDefaultLatencyBounds records, as non-blocking notes, the
// continuously probed scenarios whose path may slow down without firing.
func TestTrafficNotesDefaultLatencyBounds(t *testing.T) {
    workloads, err := prober.ParseWorkloads(TrafficWorkloads)
    if err != nil { t.Fatal(err) }
    notes := []string{}
    for _, id := range traffic.DefaultLatencyScenarios(workloads) {
        notes = append(notes, traffic.DefaultLatencyNote(id))
    }
    data, err := json.Marshal(notes)
    if err != nil { t.Fatal(err) }
    if err := os.WriteFile("` + trafficLatencyNotes + `", data, 0o600); err != nil { t.Fatal(err) }
}

// TestTrafficProbeLoadWithinCap refuses a probe set that sends more requests
// per second than the probe-load cap.
func TestTrafficProbeLoadWithinCap(t *testing.T) {
    workloads, err := prober.ParseWorkloads(TrafficWorkloads)
    if err != nil { t.Fatal(err) }
    if err := traffic.CheckProbeLoad(workloads, TrafficCatalog()); err != nil { t.Fatal(err) }
}

func TestTrafficGeneratorsDetectTheirFaultClasses(t *testing.T) {
    for _, failure := range traffic.CheckCatalog(context.Background(), TrafficCatalog()) {
        t.Error(failure)
    }
}

func TestTrafficWorkloadsRunOnlyProvidedScenarios(t *testing.T) {
    workloads, err := prober.ParseWorkloads(TrafficWorkloads)
    if err != nil { t.Fatal(err) }
    config := prober.Config{Namespace: "check", Catalog: TrafficCatalog(), Workloads: workloads}
    if _, err := prober.New(config); err != nil { t.Fatal(err) }
}

func TestTrafficDetectorsConsumeContinuousProbeWorkloads(t *testing.T) {
    workloads, err := prober.ParseWorkloads(TrafficWorkloads)
    if err != nil { t.Fatal(err) }
    purposes := map[string]traffic.Purpose{}
    for _, workload := range workloads { purposes[workload.Name] = workload.Purpose }
    // A Go-installed traffic-<workload> package is regenerated from the
    // workload files, so a missing workload means a stale package.
    for _, detector := range All() {
        consumer, ok := detector.(traffic.Consumer)
        if !ok { continue }
        for _, name := range consumer.TrafficWorkloads() {
            purpose, present := purposes[name]
            if !present {
                t.Errorf("detector %s consumes workload %q, but the file .sdo/diagnostics/traffic/workloads/%s.yaml does not exist; restore the workload or run sdo-operator memory check so Go removes the stale package .sdo/diagnostics/detectors/health/%s", detector.Spec().ID, name, name, detector.Spec().ID)
            } else if purpose != traffic.PurposeHealthProbe && purpose != traffic.PurposeLinkProbe {
                t.Errorf("detector %s consumes workload %q, whose purpose is %q, not a health-probe or link-probe", detector.Spec().ID, name, purpose)
            }
        }
    }
}
`
