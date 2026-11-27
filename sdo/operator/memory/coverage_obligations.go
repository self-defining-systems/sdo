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
	return m, m.Coverage(appmap.ParseInventory(string(data)), MapWorkloads(checkout)), true
}

// MapWorkloads lists the traffic workloads' scenario IDs and knownBroken
// declarations, as the map check reads them.
func MapWorkloads(checkout string) []appmap.Workload {
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
		if workload, ok := MapWorkload(file.Name(), content); ok {
			out = append(out, workload)
		}
	}
	return out
}

// MapWorkload reads one traffic workload from an accepted file.
func MapWorkload(file string, content []byte) (appmap.Workload, bool) {
	workload, err := parseTrafficWorkload(file, content)
	if err != nil {
		return appmap.Workload{}, false
	}
	w := appmap.Workload{Name: workload.GetName()}
	for _, scenario := range workload.GetScenarios() {
		w.Scenarios = append(w.Scenarios, scenario.GetId())
		if scenario.GetKnownBroken() != nil {
			w.KnownBroken = append(w.KnownBroken, scenario.GetId())
		}
	}
	return w, true
}

// CoverageObligations renders, for the detection designer's brief, the
// components on the user path that no probed user operation covers and that
// record no unprobed reason: the memory check's map.coverage findings. It also
// names the components no user path reaches that hold no data (workers,
// whether or not a metrics Service selects them), which only a user operation
// whose effect the probe waits for can cover. It is empty when the
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
		if !got.OnUserPath && comp.Data == nil && got.Unprobed == "" {
			workers = append(workers, comp.Name)
		}
	}
	sort.Strings(entries)
	sort.Strings(workers)
	var stores, unprobedEntries []string
	userPath, unprobed := 0, 0
	for _, comp := range m.Components {
		got := coverage.Components[comp.Name]
		if comp.HoldsData {
			stores = append(stores, comp.Name)
		}
		if got.OnUserPath {
			userPath++
			if got.Unprobed != "" {
				unprobed++
			}
		}
		if got.Entry && got.Unprobed != "" && len(got.Probes) == 0 {
			unprobedEntries = append(unprobedEntries, comp.Name)
		}
	}
	sort.Strings(stores)
	sort.Strings(unprobedEntries)
	if len(owed) == 0 && len(workers) == 0 && len(stores) == 0 && len(unprobedEntries) == 0 {
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
	if len(unprobedEntries) > 0 {
		fmt.Fprintf(&b, "\nThese components own a user entry Service and record `unprobed`, which is not allowed on an entry (map.unprobed-entry): %s. Declare an operation entering at the component's Service (EXEC for SSH commands) and probe it.\n", strings.Join(unprobedEntries, ", "))
	}
	if 2*unprobed > userPath {
		fmt.Fprintf(&b, "\n%d of %d user-path components record `unprobed`; more than half is rejected (map.unprobed-share). Cover most of them with operations.\n", unprobed, userPath)
	}
	if len(stores) > 0 {
		fmt.Fprintf(&b, "\nThe map holds data (%s), so it needs at least one gated probe (call or read-later) with a step built with `ReadsExistingRecords(path, field, pattern)` that lists records users made or the deployment seeded and checks their shape (map.records-unread). An empty list passes, so a fresh install stays healthy. The step must not template a key a write step of its scenario saved; list live and save the id from the list. Only when users keep no records, set `no_user_records: <why>` at the top level of the map.\n", strings.Join(stores, ", "))
	}
	if len(workers) > 0 {
		fmt.Fprintf(&b, "\nNo user path reaches these components and they hold no data: %s. If one does work users wait for (a worker, consumer or schedule that takes work from a queue or table users write), declare its internal operation and probe it through the user operation that hands it the work: perform that operation, then read the effect (a status that moves on, a result that becomes readable) in a step wrapped in traffic.Eventually (map.effect-unobserved). A synchronous read-back, or an edge drawn toward the worker, does not observe it. Otherwise record `unprobed: <why>`.\n", strings.Join(workers, ", "))
	}
	return b.String()
}
