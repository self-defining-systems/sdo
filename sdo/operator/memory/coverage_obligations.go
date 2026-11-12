package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// acceptedMap reads the application map, the inventory and the traffic
// workloads of a checkout and computes the map's coverage. ok is false when the
// checkout has no parsable map.
func acceptedMap(checkout string) (m appmap.Map, coverage appmap.Coverage, ok bool) {
	data, err := os.ReadFile(filepath.Join(checkout, layout.ArchitectureFile))
	if err != nil {
		return appmap.Map{}, appmap.Coverage{}, false
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		return appmap.Map{}, appmap.Coverage{}, false
	}
	m, err = appmap.Parse(region.YAML)
	if err != nil {
		return appmap.Map{}, appmap.Coverage{}, false
	}
	return m, m.Coverage(appmap.ParseInventory(string(data)), mapWorkloads(checkout)), true
}

// mapWorkloads lists the traffic workloads' scenario IDs and knownBroken
// declarations, as the map check reads them.
func mapWorkloads(checkout string) []appmap.Workload {
	directory := filepath.Join(checkout, layout.TrafficWorkloadsDir)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil
	}
	var out []appmap.Workload
	for _, file := range entries {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".yaml") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(directory, file.Name()))
		if err != nil {
			continue
		}
		workload, err := parseTrafficWorkload(file.Name(), content)
		if err != nil {
			continue
		}
		w := appmap.Workload{Name: workload.GetName()}
		for _, scenario := range workload.GetScenarios() {
			w.Scenarios = append(w.Scenarios, scenario.GetId())
			if scenario.GetKnownBroken() != nil {
				w.KnownBroken = append(w.KnownBroken, scenario.GetId())
			}
		}
		out = append(out, w)
	}
	return out
}

// CoverageObligations renders, for the detection designer's brief, the
// components on the user path that no probed user operation covers and that
// record no unprobed reason: the memory check's map.coverage findings. It also
// names the components no user path reaches that have no Service (workers),
// which only an operation through an entry can cover. It is empty when the
// checkout has no parsable map or nothing is owed.
func CoverageObligations(checkout string) string {
	m, coverage, ok := acceptedMap(checkout)
	if !ok {
		return ""
	}
	owed := coverage.Obligations()
	var entries, workers []string
	for _, comp := range m.Components {
		got := coverage.Components[comp.Name]
		if got.Entry {
			entries = append(entries, strings.Join(comp.Services, ", "))
		}
		if !got.OnUserPath && len(comp.Services) == 0 && got.Unprobed == "" {
			workers = append(workers, comp.Name)
		}
	}
	sort.Strings(entries)
	sort.Strings(workers)
	if len(owed) == 0 && len(workers) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("# Probe coverage (Go check of the accepted map, not part of the architecture file)\n\n")
	if coverage.ExposedEntries {
		fmt.Fprintf(&b, "User entry Services (exposed outside the cluster): %s.\n", strings.Join(entries, "; "))
	} else {
		b.WriteString("No mapped Service is exposed outside the cluster, so the user entry Services are those your declared operations enter at")
		if len(entries) > 0 {
			fmt.Fprintf(&b, ": %s", strings.Join(entries, "; "))
		}
		b.WriteString(".\n")
	}
	if len(owed) > 0 {
		b.WriteString("\nThese components are on the user path, but no probed operation entering at a user entry Service includes them, so a fault in them stays invisible while pods are Ready:\n")
		for _, name := range owed {
			fmt.Fprintf(&b, "- %s\n", name)
		}
		b.WriteString("\nCover each with an operation that enters at a user entry Service and whose path includes it, and a probe that exercises it. An operation entering elsewhere (a metrics scrape, an internal API) covers nothing. Record `unprobed: <why>` on the component only when no synthetic request can reach it.\n")
	}
	if len(workers) > 0 {
		fmt.Fprintf(&b, "\nNo user path reaches these components and they have no Service: %s. If users depend on them (background jobs, queues), map the edge that reaches them and cover them with an operation through an entry, such as a write-read or read-later probe, or record `unprobed: <why>`.\n", strings.Join(workers, ", "))
	}
	return b.String()
}
