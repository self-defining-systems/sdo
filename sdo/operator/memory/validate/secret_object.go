package validate

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
)

// secretDataDiagnostics rejects changed memory data files, captured corpus
// cases included, that hold a Kubernetes Secret's data. References to a Secret
// (kind and name) remain allowed. Captures never read Secrets; this keeps
// memory, which is pushed to a shared remote, free of them whatever the author.
// Rendered source output and synthetic testdata are out of scope.
func secretDataDiagnostics(root string, paths []string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, p := range paths {
		if !layout.IsMemoryPath(p) || strings.HasPrefix(p, layout.RenderedDir+"/") || slices.Contains(strings.Split(p, "/"), "testdata") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
		if err != nil {
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
		if err == nil && holdsSecretData(document, false) {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleSecretData, Path: p, Message: "file holds Kubernetes Secret data", Hint: "never store Secret values in memory; reference the Secret by kind and name"})
		}
	}
	return diagnostics
}

// holdsSecretData reports a Secret object with data or stringData, either by
// kind or as an element of a typed "secrets" collection.
func holdsSecretData(value any, inSecrets bool) bool {
	switch v := value.(type) {
	case map[string]any:
		kind, _ := v["kind"].(string)
		if inSecrets || strings.EqualFold(kind, "Secret") {
			if _, ok := v["data"]; ok {
				return true
			}
			if _, ok := v["stringData"]; ok {
				return true
			}
		}
		for key, child := range v {
			if holdsSecretData(child, strings.EqualFold(key, "secrets")) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if holdsSecretData(child, inSecrets) {
				return true
			}
		}
	}
	return false
}
