package program

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

func planDocument(raw any, format string) (map[string]any, error) {
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("ConfigMap document must be a string")
	}
	var object map[string]any
	switch format {
	case "json":
		decoder := json.NewDecoder(bytes.NewBufferString(text))
		decoder.UseNumber()
		if err := decoder.Decode(&object); err != nil {
			return nil, fmt.Errorf("invalid ConfigMap JSON document")
		}
		if decoder.Decode(new(any)) != io.EOF {
			return nil, fmt.Errorf("multiple JSON documents")
		}
	case "yaml":
		decoder := yaml.NewDecoder(bytes.NewBufferString(text))
		if err := decoder.Decode(&object); err != nil {
			return nil, fmt.Errorf("invalid ConfigMap YAML document")
		}
		if decoder.Decode(new(any)) != io.EOF {
			return nil, fmt.Errorf("multiple YAML documents")
		}
	default:
		return nil, fmt.Errorf("unsupported document format")
	}
	if object == nil {
		return nil, fmt.Errorf("ConfigMap document must be an object")
	}
	return object, nil
}

func documentEntry(object map[string]any, path []string) (any, error) {
	var current any = object
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("document path parent is not an object")
		}
		var exists bool
		current, exists = m[key]
		if !exists {
			return nil, fmt.Errorf("accepted document entry is absent")
		}
	}
	return current, nil
}

func setDocumentEntry(object map[string]any, path []string, value any) error {
	parent := object
	for _, key := range path[:len(path)-1] {
		next, ok := parent[key].(map[string]any)
		if !ok {
			return fmt.Errorf("document parent is absent or not an object")
		}
		parent = next
	}
	// Preserve the kind of an existing entry; new entries use the accepted value's kind.
	key := path[len(path)-1]
	if previous, exists := parent[key]; exists && documentKind(previous) != documentKind(value) {
		return fmt.Errorf("document value has the wrong schema kind")
	}
	parent[key] = value
	return nil
}
func documentKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return "number"
	}
}

// ApplySetFieldAction applies an already resolved value. The controller owns
// resolution and provenance; this helper owns field and document schema checks.
func ApplySetFieldAction(current map[string]any, a SetField, value any) error {
	if err := ValidateResolvedSetField(a, value); err != nil {
		return err
	}
	// Normalize numbers/maps from both Go programs and YAML, without string coercion.
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("invalid field value")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if a.DocumentFormat == "json" {
		decoder.UseNumber()
	}
	if err = decoder.Decode(&value); err != nil {
		return fmt.Errorf("invalid field value")
	}
	if a.DocumentFormat != "" {
		raw, err := FieldValue(current, a.Path)
		if err != nil {
			return err
		}
		document, err := planDocument(raw, a.DocumentFormat)
		if err != nil {
			return err
		}
		if err := setDocumentEntry(document, a.DocumentPath, value); err != nil {
			return err
		}
		var rawDocument []byte
		if a.DocumentFormat == "json" {
			rawDocument, err = json.Marshal(document)
		} else {
			rawDocument, err = yaml.Marshal(document)
		}
		if err != nil {
			return fmt.Errorf("encode ConfigMap document")
		}
		value = string(rawDocument)
	}
	if err := ApplySetField(current, a.Path, value); err != nil {
		return err
	}
	return nil
}
