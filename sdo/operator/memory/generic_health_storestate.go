package memory

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// StoreStateDetectorPrefix starts the ID of every store-state detector Go
// installs from the application map; the rest of the ID is the component.
const StoreStateDetectorPrefix = "store-state-"

// StoreStateLoad is the pod-read load Go's store readers may use, in reads per
// podread.MinInterval: pod reads share the podread.MaxQueries load budget with
// the designer's own, so Go takes half of it. Every readable store gets a
// reader; more readers mean longer intervals (readMultiples), not fewer readers.
const StoreStateLoad = podread.MaxQueries / 2

// MaxStoreStateDetectors bounds the store readers Go declares: half of
// podread.MaxDeclaredQueries. Only past it does ranking decide which exist.
const MaxStoreStateDetectors = podread.MaxDeclaredQueries / 2

// plannedStoreStateDetectors lists one store-state detector per application
// map component whose data.engine has a storestate adapter: stores a probed
// user operation depends on first (only they can explain a failing user
// request; appmap.ComponentCoverage.Probed), then authoritative stores, then
// by name. Their intervals fit StoreStateLoad with the probed stores read
// most often. A store a designer-written health detector already reads keeps
// that one reader (designerReadStores): two readers would declare its pod read
// differently and the controller would refuse both. A missing or unreadable
// map plans none: the shared check reports the map itself.
func plannedStoreStateDetectors(root string) []plannedDetector {
	parsed, coverage, ok := acceptedMap(root)
	if !ok {
		return nil
	}
	designed := designerReadStores(root)
	var stores []appmap.Component
	for _, component := range parsed.Components {
		if component.Data == nil || designed[component.Name] || !layout.ValidDetectorID(StoreStateDetectorPrefix+component.Name) {
			continue
		}
		if _, ok := storestate.AdapterFor(storestate.Engine(component.Data.Engine)); !ok {
			continue
		}
		if (podread.Target{Workload: component.Workload}).Validate() != nil {
			continue
		}
		stores = append(stores, component)
	}
	probed := func(c appmap.Component) bool { return coverage.Components[c.Name].Probed() }
	sort.SliceStable(stores, func(i, j int) bool {
		if pi, pj := probed(stores[i]), probed(stores[j]); pi != pj {
			return pi
		}
		ai, aj := stores[i].Data.Role == appmap.RoleAuthoritative, stores[j].Data.Role == appmap.RoleAuthoritative
		if ai != aj {
			return ai
		}
		return stores[i].Name < stores[j].Name
	})
	if len(stores) > MaxStoreStateDetectors {
		stores = stores[:MaxStoreStateDetectors]
	}
	onPath := 0
	for _, component := range stores {
		if probed(component) {
			onPath++
		}
	}
	kp, ku := readMultiples(onPath, len(stores)-onPath, StoreStateLoad)
	planned := make([]plannedDetector, 0, len(stores))
	for _, component := range stores {
		id := StoreStateDetectorPrefix + component.Name
		interval := time.Duration(ku) * podread.MinInterval
		if probed(component) {
			interval = time.Duration(kp) * podread.MinInterval
		}
		config := storestate.Config{
			Spec:      sdk.DetectorSpec{ID: id, Interval: interval},
			Component: component.Name, Engine: storestate.Engine(component.Data.Engine),
			Target: podread.Target{Workload: component.Workload}, ReadInterval: interval, GapWithoutBaseline: true,
		}
		// The generated constructor cannot fail on a config that passes here.
		if _, err := storestate.NewDetector(config); err != nil {
			continue
		}
		manifest := genericManifest(id, interval.String(), []layout.Watch{{APIVersion: "v1", Kind: "Pod"}})
		planned = append(planned, plannedDetector{manifest, storeStateSource(id, config)})
	}
	return planned
}

// prepareStoreStateDetectors installs the planned store readers and removes
// Go-installed ones the map no longer backs.
func prepareStoreStateDetectors(root string) error {
	wanted := map[string]bool{}
	for _, d := range plannedStoreStateDetectors(root) {
		if err := writeGenericDetector(root, d.manifest, d.source); err != nil {
			return err
		}
		wanted[d.manifest.ID] = true
	}
	return pruneStaleBootstrapDetectors(root, StoreStateDetectorPrefix, wanted)
}

func storeStateSource(id string, c storestate.Config) string {
	return fmt.Sprintf(`package storeread

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// New reads the store's value-free facts and compares them with the
// controller's promoted healthy readings. A store never read healthy is a
// coverage gap, not a finding.
func New() sdk.Detector {
	d, err := storestate.NewDetector(storestate.Config{
		Spec: sdk.DetectorSpec{
			%s
			Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}},
			Interval: %d * time.Second,
		},
		Component: %q,
		Engine: storestate.Engine(%q),
		Target: podread.Target{Workload: %q},
		ReadInterval: %d * time.Second,
		GapWithoutBaseline: true,
	})
	if err != nil {
		panic(err)
	}
	return d
}
`, genericSpec(id), int(c.Spec.Interval.Seconds()), c.Component, string(c.Engine), c.Target.Workload, int(c.ReadInterval.Seconds()))
}

// designerReadStores lists the components that health detectors Go did not
// install read through storestate: a storestate.Config literal's Component, or
// a storestate.QueryID call's component, as a string literal in a non-test
// file, however the package is imported. A reader that computes the name
// escapes it; the shared check then refuses the two differing declarations
// (podread.Queries) at publication.
func designerReadStores(root string) map[string]bool {
	stores := map[string]bool{}
	base := filepath.Join(root, layout.DetectorDir(layout.ClassHealth, ""))
	entries, err := os.ReadDir(base)
	if err != nil {
		return stores
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(base, entry.Name())
		manifest, err := os.ReadFile(filepath.Join(dir, layout.ManifestFileName))
		if err != nil {
			continue
		}
		if parsed, err := layout.ParseDetectorManifest(manifest); err != nil || parsed.OriginatingCommit == "lifecycle-bootstrap" {
			continue
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, file := range files {
			name := file.Name()
			if file.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			for _, component := range storeStateComponents(filepath.Join(dir, name)) {
				stores[component] = true
			}
		}
	}
	return stores
}

const storeStatePackage = "sdo.dev/operator/sdk/storestate"

// storeStateComponents returns the literal components file reads through
// storestate.
func storeStateComponents(file string) []string {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	alias := ""
	for _, spec := range parsed.Imports {
		if path, err := strconv.Unquote(spec.Path.Value); err != nil || path != storeStatePackage {
			continue
		}
		alias = "storestate"
		if spec.Name != nil {
			alias = spec.Name.Name
		}
	}
	if alias == "" || alias == "_" {
		return nil
	}
	// A dot import names Config and QueryID bare.
	named := func(expr ast.Expr, name string) bool {
		if alias == "." {
			ident, ok := expr.(*ast.Ident)
			return ok && ident.Name == name
		}
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && pkg.Name == alias && selector.Sel.Name == name
	}
	literal := func(expr ast.Expr) (string, bool) {
		lit, ok := expr.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return "", false
		}
		value, err := strconv.Unquote(lit.Value)
		return value, err == nil && value != ""
	}
	var components []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.CompositeLit:
			if n.Type == nil || !named(n.Type, "Config") {
				return true
			}
			for _, element := range n.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				if key, ok := field.Key.(*ast.Ident); ok && key.Name == "Component" {
					if value, ok := literal(field.Value); ok {
						components = append(components, value)
					}
				}
			}
		case *ast.CallExpr:
			if named(n.Fun, "QueryID") && len(n.Args) == 2 {
				if value, ok := literal(n.Args[1]); ok {
					components = append(components, value)
				}
			}
		}
		return true
	})
	return components
}

func isStoreStateDetector(id string) bool { return strings.HasPrefix(id, StoreStateDetectorPrefix) }
