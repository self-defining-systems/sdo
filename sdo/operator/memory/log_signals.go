package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/logsignal"
)

// LogSignalDetectorPrefix starts the ID of every Go-installed log-signal
// detector; the rest of the ID is the log signal's ID.
const LogSignalDetectorPrefix = "logs-"

// LogSignalDetectorID is the health detector Go installs for a log signal.
func LogSignalDetectorID(signal string) string { return LogSignalDetectorPrefix + signal }

// LogSignalOfDetector returns the log signal a Go-installed detector ID runs.
func LogSignalOfDetector(id string) (string, bool) {
	signal, ok := strings.CutPrefix(id, LogSignalDetectorPrefix)
	return signal, ok && logsignal.ValidID(signal)
}

// prepareLogSignalDetectors installs one health detector per log signal file
// (.sdo/diagnostics/logs/<id>.yaml) as logs-<id>, built from the file's text
// and the application map's workload of each component it names, and removes
// the Go-installed log-signal detectors whose file is gone. Like a data
// check's, the detector is a function of its inputs, so a changed file or map
// replaces it, and a detector of that ID Go did not install is never
// overwritten. A file naming a component the map does not resolve is left to
// the shared check, which reports it.
func prepareLogSignalDetectors(root string) error {
	directory := filepath.Join(root, layout.LogSignalsDir)
	entries, err := os.ReadDir(directory)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	workloads := mapWorkloads(root)
	wanted := map[string]bool{}
	for _, entry := range entries {
		kind, signal, ok := layout.Classify(layout.LogSignalsDir + "/" + entry.Name())
		if entry.IsDir() || !ok || kind != layout.KindLogSignal {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return err
		}
		parsed, err := logsignal.Parse(data)
		if err == nil {
			err = parsed.Validate(signal)
		}
		if err != nil {
			return fmt.Errorf("log signal %s: %w", signal, err)
		}
		resolved := map[string]string{}
		for _, component := range parsed.Components {
			if workloads[component] != "" {
				resolved[component] = workloads[component]
			}
		}
		if len(resolved) != len(parsed.Components) {
			continue
		}
		id := LogSignalDetectorID(signal)
		if err := replaceBootstrapDetector(root, id); err != nil {
			return err
		}
		manifest := genericManifest(id, fmt.Sprintf("%ds", int(logsignal.Interval.Seconds())), []layout.Watch{{APIVersion: logs.Watch.APIVersion, Kind: logs.Watch.Kind}})
		if err := writeGenericDetector(root, manifest, logSignalSource(id, signal, string(data), resolved)); err != nil {
			return err
		}
		wanted[id] = true
	}
	return pruneStaleBootstrapDetectors(root, LogSignalDetectorPrefix, wanted)
}

// mapWorkloads returns each application-map component's workload, or none
// when the map is missing or unreadable.
func mapWorkloads(root string) map[string]string {
	out := map[string]string{}
	data, err := os.ReadFile(filepath.Join(root, layout.ArchitectureFile))
	if err != nil {
		return out
	}
	region, found, err := appmap.Find(string(data))
	if !found || err != nil {
		return out
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		return out
	}
	for _, c := range parsed.Components {
		out[c.Name] = c.Workload
	}
	return out
}

// logSignalSource embeds the file's text and the resolved workloads in the
// detector, so the detector is bound to the accepted inputs it was built from.
func logSignalSource(id, signal, text string, workloads map[string]string) string {
	components := make([]string, 0, len(workloads))
	for component := range workloads {
		components = append(components, component)
	}
	sort.Strings(components)
	var entries strings.Builder
	for _, component := range components {
		fmt.Fprintf(&entries, "%q: %q,\n", component, workloads[component])
	}
	return fmt.Sprintf(`package synthetic

import (
	"time"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/logsignal"
)

func New() sdk.Detector {
	return logsignal.NewDetector(sdk.DetectorSpec{
		%s
		Watches: []sdk.WatchKind{logs.Watch},
		Interval: %d * time.Second,
	}, %q, %q, map[string]string{
		%s})
}
`, genericSpec(id), int(logsignal.Interval.Seconds()), signal, text, entries.String())
}
