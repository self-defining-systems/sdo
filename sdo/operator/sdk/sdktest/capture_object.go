package sdktest

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ProjectCapturedObject preserves arbitrary Kubernetes fields, including object
// identity (metadata.uid), while stripping credential payloads, last-applied
// copies and volatile write metadata.
// Discovery callers exclude Secrets entirely; this also protects recorded JSON.
func ProjectCapturedObject(raw []byte) ([]byte, error) {
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	api, _ := object["apiVersion"].(string)
	kind, _ := object["kind"].(string)
	if api == "" || kind == "" {
		return nil, fmt.Errorf("capture requires Kubernetes apiVersion and kind")
	}
	if strings.HasSuffix(kind, "List") {
		items, _ := object["items"].([]any)
		for i, item := range items {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid Kubernetes list item")
			}
			if m["apiVersion"] == nil {
				m["apiVersion"] = api
			}
			if m["kind"] == nil {
				m["kind"] = strings.TrimSuffix(kind, "List")
			}
			b, _ := json.Marshal(m)
			p, err := ProjectCapturedObject(b)
			if err != nil {
				return nil, err
			}
			if err = json.Unmarshal(p, &items[i]); err != nil {
				return nil, err
			}
		}
	} else if _, ok := object["metadata"].(map[string]any); !ok {
		return nil, fmt.Errorf("capture requires resource metadata")
	}
	redactObjectTree(object)
	return json.Marshal(object)
}
func redactObjectTree(value any) {
	switch v := value.(type) {
	case []any:
		for _, item := range v {
			redactObjectTree(item)
		}
	case map[string]any:
		if v["kind"] == "Secret" {
			// Whitelist only metadata and key names for Secrets, including nested objects.
			for key := range v {
				if key != "apiVersion" && key != "kind" && key != "metadata" && key != "data" && key != "stringData" {
					delete(v, key)
				}
			}
			for _, key := range []string{"data", "stringData"} {
				if data, ok := v[key].(map[string]any); ok {
					for k := range data {
						data[k] = ""
					}
				}
			}
		}
		if meta, ok := v["metadata"].(map[string]any); ok {
			// metadata.uid stays: consumers distinguish an in-place repair from a
			// same-name recreation by UID (undetected causes, replaced Pods).
			delete(meta, "managedFields")
			delete(meta, "generation")
			if annotations, ok := meta["annotations"].(map[string]any); ok {
				delete(annotations, "kubectl.kubernetes.io/last-applied-configuration")
			}
		}
		if name, ok := v["name"].(string); ok && sensitiveEnvironmentName.MatchString(name) {
			if _, exists := v["value"]; exists {
				v["value"] = ""
			}
			delete(v, "valueFrom")
		}
		// ConfigMap values follow the existing capture-only redaction policy.
		if v["kind"] == "ConfigMap" {
			for _, key := range []string{"data", "binaryData"} {
				if data, ok := v[key].(map[string]any); ok {
					for k, value := range data {
						if key == "data" {
							data[k] = capturedConfigValue(k, value)
						} else {
							data[k] = ""
						}
					}
				}
			}
			if meta, ok := v["metadata"].(map[string]any); ok {
				annotations, ok := meta["annotations"].(map[string]any)
				if !ok {
					annotations = map[string]any{}
					meta["annotations"] = annotations
				}
				annotations["sdo.dev/configmap-values-redacted"] = "true"
			}
		}
		if involved, ok := v["involvedObject"].(map[string]any); ok {
			delete(involved, "uid")
			delete(involved, "resourceVersion")
		}
		for _, item := range v {
			redactObjectTree(item)
		}
	}
}

// Structured ConfigMaps can contain operational flags. Keep document structure
// and redact credential-named entries, while opaque values remain key-only.
func capturedConfigValue(key string, value any) any {
	text, ok := value.(string)
	if !ok || sensitiveEnvironmentName.MatchString(key) {
		return ""
	}
	if text == "true" || text == "false" {
		return text
	}
	var document map[string]any
	if json.Unmarshal([]byte(text), &document) != nil || document == nil {
		return ""
	}
	var scrub func(any)
	scrub = func(value any) {
		switch v := value.(type) {
		case []any:
			for _, item := range v {
				scrub(item)
			}
		case map[string]any:
			for key, item := range v {
				if sensitiveEnvironmentName.MatchString(key) {
					v[key] = ""
				} else {
					scrub(item)
				}
			}
		}
	}
	scrub(document)
	redactObjectTree(document)
	raw, err := json.Marshal(document)
	if err != nil {
		return ""
	}
	return string(raw)
}
