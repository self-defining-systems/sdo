package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil || envelope == nil {
		return nil, fmt.Errorf("tool request: expected JSON object")
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
