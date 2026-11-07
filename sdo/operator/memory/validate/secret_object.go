package validate

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
)

// secretDataDiagnostics rejects changed memory data files, captured corpus
// cases included, that hold a Kubernetes Secret's data. References to a Secret
// (kind and name) remain allowed. Captures never read Secrets; this keeps
// memory, which is pushed to a shared remote, free of them whatever the author.
// Synthetic testdata is out of scope; deployment manifests and values are
// checked like any other memory file.
func secretDataDiagnostics(root string, paths []string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, p := range paths {
		if !layout.IsMemoryPath(p) || slices.Contains(strings.Split(p, "/"), "testdata") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
		if err != nil {
			continue
		}
		if material := secretMaterial(p, data); material != "" {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleSecretMaterial, Path: p, Message: "file holds credential-looking material: " + material, Hint: "never store credentials in memory; reference a Secret by kind and name or template the value"})
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

var (
	pemPrivateKey = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----`)
	// Well-known credential shapes: cloud access key IDs, forge and chat tokens, JWTs.
	tokenShapes = regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b|\bgh[pousr]_[A-Za-z0-9]{30,}|\bglpat-[A-Za-z0-9_-]{20,}|\bxox[abprs]-[A-Za-z0-9-]{10,}|\bsk-[A-Za-z0-9_-]{32,}|\beyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]{8,}`)
	// A credential-named key with a literal value on the same line.
	credentialKeyValue = regexp.MustCompile(`(?im)^[\s+-]*(?:- )?["']?[\w.-]*(?:password|passwd|token|api[_-]?key|private[_-]?key|access[_-]?key|client[_-]?secret|secret[_-]?key|secretAccessKey)["']?\s*[:=]\s*(\S.*)$`)
)

// secretMaterial names credential-looking content of an agent-authored memory
// file, or "". Key shapes and PEM blocks are checked in every memory file;
// credential-named keys with literal values only in deployment inputs, where a
// chart value is a key and a value, not prose. Imported upstream inputs
// (unclassified deployment paths) are not judged: they are not agent-authored.
func secretMaterial(p string, data []byte) string {
	if strings.HasPrefix(p, layout.DeploymentDir+"/") {
		if _, _, authored := layout.Classify(p); !authored {
			return ""
		}
	}
	text := string(data)
	switch {
	case pemPrivateKey.MatchString(text):
		return "PEM private key"
	case tokenShapes.MatchString(text):
		return "access token or key"
	}
	if strings.HasPrefix(p, layout.DeploymentDir+"/") {
		for _, m := range credentialKeyValue.FindAllStringSubmatch(text, -1) {
			if literalCredentialValue(m[1]) {
				return "credential-named key with a literal value"
			}
		}
	}
	return ""
}

// literalCredentialValue reports a value that is neither empty, a template or
// variable reference, nor a non-string scalar.
func literalCredentialValue(raw string) bool {
	value := strings.TrimSpace(raw)
	if i := strings.Index(value, " #"); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	value = strings.Trim(value, "\"',")
	switch strings.ToLower(value) {
	case "", "null", "~", "true", "false", "|", ">", "|-", ">-", "{}", "[]":
		return false
	}
	return !strings.ContainsAny(value, "{$") && !strings.HasPrefix(value, "<")
}
