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

	"gopkg.in/yaml.v3"
)

var allowedGeneratorImports = map[string]bool{
	"bytes": true, "context": true, "encoding/base64": true, "encoding/json": true,
	"errors": true, "fmt": true, "math": true, "math/rand/v2": true,
	"net/url": true, "regexp": true, "sort": true, "strconv": true,
	"strings": true, "time": true, "unicode": true, "unicode/utf8": true,
	"sdo.dev/operator/sdk/traffic": true,
}

var forbiddenTimeCalls = map[string]bool{
	"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true,
	"AfterFunc": true, "Tick": true, "NewTimer": true, "NewTicker": true,
}

var serviceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

func generateTraffic(root, module string) (bool, error) {
	base := filepath.Join(root, "traffic")
	files, err := goSourceFiles(filepath.Join(base, "generators"))
	if err != nil {
		return false, err
	}
	workloads, err := loadTrafficWorkloads(filepath.Join(base, "workloads"))
	if err != nil {
		return false, err
	}
	if len(files) == 0 {
		if len(workloads) > 0 {
			return false, fmt.Errorf("traffic workloads exist but diagnostics traffic/generators has no Go package")
		}
		return false, os.WriteFile(filepath.Join(root, "generated", "traffic_test.go"), []byte(noTrafficTest), 0o644)
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
		"func TrafficCatalog() traffic.Catalog {\n" + catalog + "\treturn catalog\n}\n\n" +
		"var TrafficWorkloads = map[string][]byte{\n" + items + "}\n"
	if err := os.WriteFile(filepath.Join(root, "generated", "traffic.go"), []byte(source), 0o644); err != nil {
		return false, err
	}
	if err := os.WriteFile(filepath.Join(root, "generated", "traffic_test.go"), []byte(trafficTest), 0o644); err != nil {
		return false, err
	}
	mainDir := filepath.Join(root, "cmd", "prober")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		return false, err
	}
	main := "package main\n\nimport (\n\t\"sdo.dev/operator/runtime/prober\"\n\t" + strconv.Quote(module+"/generated") + "\n)\n\nfunc main() { prober.Main(generated.TrafficCatalog(), generated.TrafficWorkloads) }\n"
	return true, os.WriteFile(filepath.Join(mainDir, "main.go"), []byte(main), 0o644)
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

func loadTrafficWorkloads(dir string) (map[string]json.RawMessage, error) {
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
		workloads[stem] = encoded
	}
	return workloads, nil
}

func validateGeneratorSources(base, module string) error {
	allowed := map[string]bool{}
	for path := range allowedGeneratorImports {
		allowed[path] = true
	}
	allowed[module+"/traffic/generators"] = true
	return filepath.WalkDir(filepath.Join(base, "generators"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
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
			if err != nil || !allowed[name] {
				return fmt.Errorf("%s: generators may not import %q", path, name)
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
		if forbidden != "" {
			return fmt.Errorf("%s: generators may not call %s; use the engine's rng and clock", path, forbidden)
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
    "testing"
    "sdo.dev/operator/runtime/prober"
    "sdo.dev/operator/sdk/traffic"
)

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
    for _, detector := range All() {
        consumer, ok := detector.(traffic.Consumer)
        if !ok { continue }
        for _, name := range consumer.TrafficWorkloads() {
            if purposes[name] != traffic.PurposeHealthProbe && purposes[name] != traffic.PurposeLinkProbe {
                t.Errorf("detector %s consumes %q, which is not a health-probe workload", detector.Spec().ID, name)
            }
        }
    }
}
`
