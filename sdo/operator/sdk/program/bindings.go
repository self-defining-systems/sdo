package program

import (
	"bytes"
	"encoding/json"
	"maps"
)

func (b Bindings) Clone() Bindings {
	return Bindings{Objects: maps.Clone(b.Objects), Values: maps.Clone(b.Values)}
}
func (b Bindings) Equal(other Bindings) bool {
	if !maps.Equal(b.Objects, other.Objects) {
		return false
	}
	if len(b.Values) == 0 && len(other.Values) == 0 {
		return true
	}
	left, leftErr := json.Marshal(b.Values)
	right, rightErr := json.Marshal(other.Values)
	return leftErr == nil && rightErr == nil && bytes.Equal(left, right)
}

// UnmarshalJSON imports object-only corpus bindings from existing recordings.
// New recordings always use explicit objects and values maps.
func (b *Bindings) UnmarshalJSON(raw []byte) error {
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		return err
	}
	type plain Bindings
	if _, ok := shape["objects"]; ok {
		return json.Unmarshal(raw, (*plain)(b))
	}
	if _, ok := shape["values"]; ok {
		return json.Unmarshal(raw, (*plain)(b))
	}
	if string(raw) == "null" {
		*b = Bindings{}
		return nil
	}
	var objects ObjectBindings
	if err := json.Unmarshal(raw, &objects); err != nil {
		return err
	}
	*b = Bindings{Objects: objects}
	return nil
}
