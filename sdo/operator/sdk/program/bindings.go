package program

import (
	"bytes"
	"encoding/json"
	"maps"

	"sdo.dev/operator/sdk"
)

func (b Bindings) Clone() Bindings {
	return Bindings{Objects: maps.Clone(b.Objects), Values: maps.Clone(b.Values)}
}

// Equal compares object bindings through the shared citation parser and scalar
// values through their JSON representation. Omitted known API versions are
// equivalent to their defaults; namespace and explicit versions remain exact.
func (b Bindings) Equal(other Bindings) bool {
	// Object identity must not depend on whether a binding came from typed
	// controller state, a recorded JSON object, or an agent citation. Use the
	// citation parser on both sides to infer only its known default versions.
	if len(b.Objects) != len(other.Objects) {
		return false
	}
	for name, ref := range b.Objects {
		otherRef, exists := other.Objects[name]
		if !exists {
			return false
		}
		left, leftErr := canonicalBindingObject(ref)
		right, rightErr := canonicalBindingObject(otherRef)
		if leftErr != nil || rightErr != nil || left != right {
			return false
		}
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

func canonicalBindingObject(ref sdk.ObjectRef) (sdk.ObjectRef, error) {
	raw, err := json.Marshal(ref)
	if err != nil {
		return sdk.ObjectRef{}, err
	}
	return sdk.ParseObjectRef(string(raw))
}
