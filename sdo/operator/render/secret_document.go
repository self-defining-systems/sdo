package render

import (
	"encoding/json"
	"strings"

	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

// SecretDocumentIdentity finds a Secret document or list item in evidence.
// It returns identity only and never examines arbitrary nested references.
func SecretDocumentIdentity(document string) (string, string, bool) {
	var decoded string
	if json.Unmarshal([]byte(document), &decoded) == nil {
		document = decoded
	}
	reader := yamlutil.NewYAMLOrJSONDecoder(strings.NewReader(document), 4096)
	for {
		var object map[string]any
		if reader.Decode(&object) != nil {
			break
		}
		if kind, _ := object["kind"].(string); kind == "Secret" {
			metadata, _ := object["metadata"].(map[string]any)
			name, _ := metadata["name"].(string)
			namespace, _ := metadata["namespace"].(string)
			return namespace, name, true
		} else if strings.HasSuffix(kind, "List") {
			items, _ := object["items"].([]any)
			for _, item := range items {
				if kind == "SecretList" {
					if child, ok := item.(map[string]any); ok && child["kind"] == nil {
						child["kind"] = "Secret"
					}
				}
				body, _ := json.Marshal(item)
				if namespace, name, ok := SecretDocumentIdentity(string(body)); ok {
					return namespace, name, true
				}
			}
		}
	}
	return "", "", false
}
