package runtime

import (
	"slices"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// appStateReaderSource is an app-state store that can say what each reader
// observed and which Pods its pod reads select.
type appStateReaderSource interface {
	AppStateReaders() []AppStateReaderStatus
}

// readerComponents resolves each app-state reader to the map component whose
// workload owns the Pods its pod reads select (the reader's declared targets
// against the snapshot's Pods, then the workload the Pod belongs to). A reader
// whose targets select no Pod, or Pods of more than one component, stays
// unresolved: its rows are unplaced, never guessed onto a node.
func readerComponents(view detectReads, m program.AppMap, namespace string, readers []AppStateReaderStatus) map[string]string {
	out := map[string]string{}
	if namespace == "" {
		namespace = view.Namespace()
	}
	pods := view.Pods()
	for _, reader := range readers {
		var found []string
		for _, target := range reader.Targets {
			for _, pod := range pods {
				if pod.Namespace != namespace {
					continue
				}
				if _, ok := target.Select(pod); !ok {
					continue
				}
				component, ok := m.ComponentOf(view, sdk.ObjectRef{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name})
				if ok && !slices.Contains(found, component.Name) {
					found = append(found, component.Name)
				}
			}
		}
		if len(found) == 1 {
			out[reader.Reader] = found[0]
		}
	}
	return out
}

// appStateCoverage says, per component with a bound app-state reader, whether
// its state was observed: "" when some reader of it read and holds a baseline,
// else why not (the reader's read failed, it has not read yet, no baseline).
func appStateCoverage(view detectReads, m program.AppMap, namespace string, readers []AppStateReaderStatus) map[string]string {
	bound := readerComponents(view, m, namespace, readers)
	out := map[string]string{}
	for _, reader := range readers {
		component := bound[reader.Reader]
		if component == "" {
			continue
		}
		reason := ""
		switch {
		case reader.Err != "":
			reason = "app-state reader " + reader.Reader + " unavailable: " + redactDetectText(reader.Err, 70)
		case !reader.Read:
			reason = "app-state reader " + reader.Reader + " has not read yet"
		case !reader.Baselined:
			reason = "app-state reader " + reader.Reader + ": no baseline yet"
		}
		if _, seen := out[component]; !seen || reason == "" {
			out[component] = reason
		}
	}
	return out
}
