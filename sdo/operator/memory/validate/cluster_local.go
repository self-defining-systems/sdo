package validate

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// clusterLocalKeys are the field kinds that describe one cluster's live state
// rather than source or learning: object incarnation identity, allocated
// addresses and live probe times. Each controller recomputes such values into
// its own state; shared memory never carries them. Extend this list (and its
// sweep test) when a new kind is found.
var clusterLocalKeys = []string{
	"uid", "serviceUID", "resourceVersion", "creationTimestamp",
	"clusterIP", "clusterIPs", "podIP", "podIPs", "hostIP", "hostIPs",
	"capturedAt", "observedAt",
}

// clusterLocalDiagnostics rejects changed memory data files that carry a
// cluster-local value. Captured corpus cases and outcomes are incident-bound
// learning records, rendered objects are source output, and testdata holds
// synthetic fixtures; none is a live reference another cluster would misread.
// Deleting a file (stripping a legacy capture) is always allowed.
func clusterLocalDiagnostics(root string, paths []string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, p := range paths {
		if !layout.IsMemoryPath(p) || strings.HasPrefix(p, layout.CorpusDir+"/") || strings.HasPrefix(p, layout.OutcomesDir+"/") || strings.HasPrefix(p, layout.RenderedDir+"/") || slices.Contains(strings.Split(p, "/"), "testdata") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
		if err != nil {
			continue
		}
		if p == memory.LegacyReachabilityBaselinePath {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleClusterLocal, Path: p, Message: "the reachability baseline is per-cluster controller state, not memory", Hint: "delete this file; each controller measures its own baseline at startup"})
			continue
		}
		var document any
		switch path.Ext(p) {
		case ".json":
			err = json.Unmarshal(data, &document)
		case ".yaml", ".yml":
			err = yaml.Unmarshal(data, &document)
		default:
			continue
		}
		if err != nil {
			continue
		}
		if key, ok := clusterLocalKey(document); ok {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleClusterLocal, Path: p, Message: fmt.Sprintf("field %q holds a value from one cluster's live state", key), Hint: "keep cluster-specific values (UIDs, allocated IPs, probe times) out of memory; record what to measure, not one cluster's measurement"})
		}
	}
	return diagnostics
}

func clusterLocalKey(value any) (string, bool) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if slices.Contains(clusterLocalKeys, key) && clusterValue(key, child) {
				return key, true
			}
			if found, ok := clusterLocalKey(child); ok {
				return found, true
			}
		}
	case []any:
		for _, child := range v {
			if found, ok := clusterLocalKey(child); ok {
				return found, true
			}
		}
	}
	return "", false
}

// clusterValue excludes empty values and the source-declared headless marker.
func clusterValue(key string, value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case string:
		return v != "" && !(strings.HasPrefix(key, "clusterIP") && v == "None")
	case []any:
		for _, item := range v {
			if clusterValue(key, item) {
				return true
			}
		}
		return false
	}
	return true
}
