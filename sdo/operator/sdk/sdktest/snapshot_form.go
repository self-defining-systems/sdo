package sdktest

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

// ObjectsOnlySnapshotVersion stores every captured object once, in Objects.
// Loading rebuilds the typed lists from the namespace-local objects.
const ObjectsOnlySnapshotVersion = 2

// snapshotFields is Snapshot without its JSON methods.
type snapshotFields Snapshot

// MarshalJSON writes a captured snapshot (version 1 or later) in the
// objects-only form whenever its typed lists are exactly the projection of its
// namespace-local objects. Any other snapshot keeps its typed lists unchanged.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	fields := snapshotFields(s)
	compact, err := fields.typedListsDerived()
	if err != nil {
		return nil, err
	}
	if compact {
		fields.ObjectSnapshotVersion = ObjectsOnlySnapshotVersion
		for _, field := range sdk.TypedObjectFields(&fields) {
			field.SetZero()
		}
	} else if fields.ObjectSnapshotVersion >= ObjectsOnlySnapshotVersion {
		// Typed lists that objects cannot rebuild must be loaded as written.
		fields.ObjectSnapshotVersion = 1
	}
	return json.Marshal(fields)
}

// UnmarshalJSON reads both the version 1 form and the objects-only form.
func (s *Snapshot) UnmarshalJSON(data []byte) error {
	var fields snapshotFields
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	return fields.into(s)
}

// decodeSnapshotStrict rejects unknown fields; Snapshot.UnmarshalJSON cannot
// see a caller decoder's DisallowUnknownFields setting.
func decodeSnapshotStrict(raw []byte) (Snapshot, error) {
	var fields snapshotFields
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fields); err != nil {
		return Snapshot{}, err
	}
	var snapshot Snapshot
	return snapshot, fields.into(&snapshot)
}

func (f *snapshotFields) into(s *Snapshot) error {
	if f.ObjectSnapshotVersion >= ObjectsOnlySnapshotVersion {
		for _, field := range sdk.TypedObjectFields(f) {
			if field.Len() > 0 {
				return fmt.Errorf("objects-only snapshot (version %d) must not carry typed lists", f.ObjectSnapshotVersion)
			}
		}
		if err := sdk.DecodeTypedObjects(f, f.localObjects()); err != nil {
			return err
		}
	}
	*s = Snapshot(*f)
	return nil
}

func (f *snapshotFields) localObjects() []unstructured.Unstructured {
	var local []unstructured.Unstructured
	for _, object := range f.Objects {
		if object.GetNamespace() == f.NamespaceName {
			local = append(local, object)
		}
	}
	return local
}

// typedListsDerived reports whether every typed list holds exactly the
// namespace-local objects of its kind, in any order. Typed accessors sort.
func (f *snapshotFields) typedListsDerived() (bool, error) {
	if f.ObjectSnapshotVersion < 1 {
		return false, nil
	}
	var derived snapshotFields
	if err := sdk.DecodeTypedObjects(&derived, f.localObjects()); err != nil {
		return false, err
	}
	have, want := sdk.TypedObjectFields(f), sdk.TypedObjectFields(&derived)
	for index := range have {
		left, err := encodedItems(have[index])
		if err != nil {
			return false, err
		}
		right, err := encodedItems(want[index])
		if err != nil {
			return false, err
		}
		if !slices.Equal(left, right) {
			return false, nil
		}
	}
	return true, nil
}

func encodedItems(list reflect.Value) ([]string, error) {
	items := make([]string, list.Len())
	for index := range items {
		raw, err := json.Marshal(list.Index(index).Interface())
		if err != nil {
			return nil, err
		}
		items[index] = string(raw)
	}
	slices.Sort(items)
	return items, nil
}

// CanonicalSnapshot returns the stored form of a snapshot file. The version 1
// and objects-only forms of one captured snapshot have the same canonical bytes.
func CanonicalSnapshot(raw []byte) ([]byte, error) {
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, err
	}
	return json.Marshal(snapshot)
}

// SnapshotDigest hashes a snapshot's canonical form, so provenance compares
// snapshot content rather than one file layout.
func SnapshotDigest(raw []byte) (string, error) {
	canonical, err := CanonicalSnapshot(raw)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}

// SnapshotMatchesDigest accepts a recorded digest of either the exact file bytes
// (cases written before the objects-only form) or the canonical snapshot.
func SnapshotMatchesDigest(raw []byte, digest string) bool {
	if digest == "" {
		return false
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) == digest {
		return true
	}
	canonical, err := SnapshotDigest(raw)
	return err == nil && canonical == digest
}
