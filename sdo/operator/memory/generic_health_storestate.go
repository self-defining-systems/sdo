package memory

import (
	"fmt"
	"sort"
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
// most often. A missing or unreadable map plans none: the shared check reports
// the map itself.
func plannedStoreStateDetectors(root string) []plannedDetector {
	parsed, coverage, ok := acceptedMap(root)
	if !ok {
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

func isStoreStateDetector(id string) bool { return strings.HasPrefix(id, StoreStateDetectorPrefix) }
