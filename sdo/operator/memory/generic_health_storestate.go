package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/storestate"
)

// StoreStateDetectorPrefix starts the ID of every store-state detector Go
// installs from the application map; the rest of the ID is the component.
const StoreStateDetectorPrefix = "store-state-"

// MaxStoreStateDetectors bounds the store reads Go installs. Each is one pod
// read, and pod reads share podread.MaxQueries with the designer's own, so Go
// takes at most half of that budget.
const MaxStoreStateDetectors = podread.MaxQueries / 2

// storeStateInterval is the detector and pod-read interval of a Go-installed
// store reader: the pod-read floor.
const storeStateInterval = podread.MinInterval

// plannedStoreStateDetectors lists one store-state detector per application
// map component whose data.engine has a storestate adapter, authoritative
// stores first, then by name, capped at MaxStoreStateDetectors. A missing or
// unreadable map plans none: the shared check reports the map itself.
func plannedStoreStateDetectors(root string) []plannedDetector {
	data, err := os.ReadFile(filepath.Join(root, layout.ArchitectureFile))
	if err != nil {
		return nil
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		return nil
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		return nil
	}
	var stores []appmap.Component
	for _, component := range parsed.Components {
		if component.Data == nil || !layout.ValidDetectorID(StoreStateDetectorPrefix+component.Name) {
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
	sort.SliceStable(stores, func(i, j int) bool {
		ai, aj := stores[i].Data.Role == appmap.RoleAuthoritative, stores[j].Data.Role == appmap.RoleAuthoritative
		if ai != aj {
			return ai
		}
		return stores[i].Name < stores[j].Name
	})
	if len(stores) > MaxStoreStateDetectors {
		stores = stores[:MaxStoreStateDetectors]
	}
	planned := make([]plannedDetector, 0, len(stores))
	for _, component := range stores {
		id := StoreStateDetectorPrefix + component.Name
		config := storestate.Config{
			Spec:      sdk.DetectorSpec{ID: id, Interval: storeStateInterval},
			Component: component.Name, Engine: storestate.Engine(component.Data.Engine),
			Target: podread.Target{Workload: component.Workload}, ReadInterval: storeStateInterval, GapWithoutBaseline: true,
		}
		// The generated constructor cannot fail on a config that passes here.
		if _, err := storestate.NewDetector(config); err != nil {
			continue
		}
		manifest := genericManifest(id, storeStateInterval.String(), []layout.Watch{{APIVersion: "v1", Kind: "Pod"}})
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
`, genericSpec(id), int(storeStateInterval.Seconds()), c.Component, string(c.Engine), c.Target.Workload, int(c.ReadInterval.Seconds()))
}

func isStoreStateDetector(id string) bool { return strings.HasPrefix(id, StoreStateDetectorPrefix) }
