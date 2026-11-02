package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sdo.dev/operator/sdk"
)

// Public tools accept ordinary JSON values. The existing typed selection
// contract carries canonical strings internally; only this boundary encodes them.
func menuNormalizeParameters(raw json.RawMessage, path string) (json.RawMessage, error) {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, fmt.Errorf("%s: expected object mapping parameter names to JSON values", path)
	}
	encoded := map[string]string{}
	for name, value := range values {
		if string(value) == "null" {
			return nil, fmt.Errorf("%s.%s: expected string, number, boolean, object or array", path, name)
		}
		var text string
		if json.Unmarshal(value, &text) == nil {
			encoded[name] = text
			continue
		}
		var compact any
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		if err := decoder.Decode(&compact); err != nil {
			return nil, fmt.Errorf("%s.%s: expected JSON value", path, name)
		}
		canonical, err := json.Marshal(compact)
		if err != nil {
			return nil, err
		}
		encoded[name] = string(canonical)
	}
	return json.Marshal(encoded)
}

func menuNormalizeInput(data []byte) ([]byte, error) {
	if err := menuRejectDuplicateFields(data); err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil || envelope == nil {
		return nil, fmt.Errorf("tool request: expected JSON object")
	}
	// Source inspection, recorded-action capture and program bindings all
	// cross the same parser before the typed request is decoded.
	for _, name := range []string{"ref", "refs", "bindings"} {
		raw, exists := envelope[name]
		if !exists || string(raw) == "null" {
			continue
		}
		var normalized json.RawMessage
		var err error
		switch name {
		case "ref":
			normalized, err = menuNormalizeCitation(raw)
		case "refs":
			var refs []json.RawMessage
			if err = json.Unmarshal(raw, &refs); err == nil {
				for i, ref := range refs {
					refs[i], err = menuNormalizeCitation(ref)
					if err != nil {
						break
					}
				}
				if err == nil {
					normalized, err = json.Marshal(refs)
				}
			}
		case "bindings":
			normalized, err = menuNormalizeObjectBindings(raw)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		envelope[name] = normalized
	}
	if raw, ok := envelope["parameters"]; ok {
		normalized, err := menuNormalizeParameters(raw, "parameters")
		if err != nil {
			return nil, err
		}
		envelope["parameters"] = normalized
	}
	if raw, ok := envelope["selection_set"]; ok {
		var set map[string]json.RawMessage
		if err := json.Unmarshal(raw, &set); err != nil || set == nil {
			return nil, fmt.Errorf("selection_set: expected object")
		}
		if raw, ok := set["options"]; ok {
			var options []map[string]json.RawMessage
			if err := json.Unmarshal(raw, &options); err != nil || options == nil {
				return nil, fmt.Errorf("selection_set.options: expected array of option objects")
			}
			for i, option := range options {
				if option == nil {
					return nil, fmt.Errorf("selection_set.options[%d]: expected object", i)
				}
				if raw, ok := option["parameters"]; ok {
					normalized, err := menuNormalizeParameters(raw, fmt.Sprintf("selection_set.options[%d].parameters", i))
					if err != nil {
						return nil, err
					}
					option["parameters"] = normalized
				}
			}
			set["options"], _ = json.Marshal(options)
		}
		envelope["selection_set"], _ = json.Marshal(set)
	}
	return json.Marshal(envelope)
}

func menuNormalizeCitation(raw json.RawMessage) (json.RawMessage, error) {
	text := string(raw)
	var citation string
	if json.Unmarshal(raw, &citation) == nil {
		text = citation
	}
	ref, err := sdk.ParseObjectRef(text)
	if err != nil {
		return nil, err
	}
	return json.Marshal(ref)
}

func menuNormalizeObjectBindings(raw json.RawMessage) (json.RawMessage, error) {
	var bindings map[string]json.RawMessage
	if err := json.Unmarshal(raw, &bindings); err != nil {
		return nil, err
	}
	if objects, ok := bindings["objects"]; ok {
		normalized, err := menuNormalizeObjectBindings(objects)
		if err != nil {
			return nil, err
		}
		bindings["objects"] = normalized
		return json.Marshal(bindings)
	}
	if _, ok := bindings["values"]; ok {
		return raw, nil
	}
	for name, ref := range bindings {
		normalized, err := menuNormalizeCitation(ref)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		bindings[name] = normalized
	}
	return json.Marshal(bindings)
}

// encoding/json otherwise keeps the last duplicate key. Reject that ambiguity
// before any map projection can erase it, including nested action references.
func menuRejectDuplicateFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var visit func() error
	visit = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		start := decoder.InputOffset() - 1
		seen := map[string]bool{}
		duplicate := false
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok {
					return fmt.Errorf("tool input: expected object key")
				}
				if seen[name] {
					duplicate = true
				}
				seen[name] = true
			}
			if err := visit(); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		if duplicate {
			reference := &sdk.ObjectRefError{Code: sdk.ObjectRefMalformed, Input: string(data[start:decoder.InputOffset()])}
			return fmt.Errorf("ambiguous tool input: duplicate field; %w", reference)
		}
		return nil
	}
	if err := visit(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("tool input: expected one JSON document")
	}
	return nil
}
