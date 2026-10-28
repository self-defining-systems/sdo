package program

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"sdo.dev/operator/sdk"
)

// KeyedListReplacementError refuses RFC 7396 replacement of a Kubernetes keyed
// list. Use a strategic patch to preserve members and their unspecified fields.
type KeyedListReplacementError struct {
	Ref  sdk.ObjectRef
	Path string
}

func (e *KeyedListReplacementError) Error() string {
	return fmt.Sprintf("JSON merge patch would replace keyed list %s on %s/%s; use a strategic patch or identity-selected list edits", e.Path, e.Ref.Kind, e.Ref.Name)
}

func validateMergeLists(a PatchObject) error {
	typed, err := patchScheme.New(schema.FromAPIVersionAndKind(a.Ref.APIVersion, a.Ref.Kind))
	if err != nil {
		return nil
	} // Custom resources have no built-in strategic schema.
	meta, err := strategicpatch.NewPatchMetaFromStruct(typed)
	if err != nil {
		return err
	}
	var patch map[string]any
	if err = json.Unmarshal(a.Patch, &patch); err != nil {
		return err
	}
	var walk func(map[string]any, strategicpatch.LookupPatchMeta, string) error
	walk = func(object map[string]any, meta strategicpatch.LookupPatchMeta, path string) error {
		for key, value := range object {
			pointer := path + "/" + key
			if _, info, e := meta.LookupPatchMetadataForSlice(key); e == nil && info.GetPatchMergeKey() != "" {
				return &KeyedListReplacementError{Ref: a.Ref, Path: pointer}
			}
			if child, ok := value.(map[string]any); ok {
				nested, _, e := meta.LookupPatchMetadataForStruct(key)
				if e == nil {
					if err := walk(child, nested, pointer); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
	return walk(patch, meta, "")
}

// MergeObject projects apply-style updates using the built-in keyed-list schema
// when available. Custom resources retain JSON merge semantics.
func MergeObject(object map[string]any, ref sdk.ObjectRef, desired json.RawMessage) (map[string]any, error) {
	kind := "merge"
	if _, err := patchScheme.New(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind)); err == nil {
		kind = "strategic"
	}
	return ProjectPatch(object, PatchObject{Ref: ref, Type: kind, Patch: desired})
}

// RawMergeSchemaError refuses an opaque document merge without a trustworthy
// target schema. Use a typed object action for unknown kinds or subresources.
type RawMergeSchemaError struct{ Path, Reason string }

func (e *RawMergeSchemaError) Error() string {
	return fmt.Sprintf("cannot validate merge schema for %s: %s", e.Path, e.Reason)
}

// Resolve built-in schemas from the URL, never from an unverified caller hint.
// Unknown resources and subresources need a typed action rather than an opaque
// document merge whose keyed-list semantics cannot be checked locally.
func rawMergeTarget(r KubernetesRequest, parts []string, index int) (sdk.ObjectRef, error) {
	if len(parts) != index+2 {
		return sdk.ObjectRef{}, &RawMergeSchemaError{Path: r.Path, Reason: "requires a named resource without a subresource"}
	}
	version := parts[1]
	if parts[0] == "apis" {
		version = parts[1] + "/" + parts[2]
	}
	namespace := ""
	if index >= 4 && parts[index-2] == "namespaces" {
		namespace = parts[index-1]
	}
	for gvk := range patchScheme.AllKnownTypes() {
		resource, _ := meta.UnsafeGuessKindToResource(gvk)
		if gvk.GroupVersion().String() != version || resource.Resource != parts[index] {
			continue
		}
		ref := sdk.ObjectRef{APIVersion: version, Kind: gvk.Kind, Namespace: namespace, Name: parts[index+1]}
		if r.Ref != (sdk.ObjectRef{}) && r.Ref != ref {
			return sdk.ObjectRef{}, &RawMergeSchemaError{Path: r.Path, Reason: "reference must match API URL"}
		}
		return ref, nil
	}
	return sdk.ObjectRef{}, &RawMergeSchemaError{Path: r.Path, Reason: "unknown target schema; use a typed object action"}
}

// Apply-style Service updates select named ports by name. Kubernetes' native
// strategic key is the number, which changes during an ordinary port update.
type namedPortPatchMeta struct {
	strategicpatch.LookupPatchMeta
	spec bool
}

func (m namedPortPatchMeta) LookupPatchMetadataForStruct(key string) (strategicpatch.LookupPatchMeta, strategicpatch.PatchMeta, error) {
	child, info, err := m.LookupPatchMeta.LookupPatchMetadataForStruct(key)
	if !m.spec && key == "spec" && err == nil {
		child = namedPortPatchMeta{LookupPatchMeta: child, spec: true}
	}
	return child, info, err
}
func (m namedPortPatchMeta) LookupPatchMetadataForSlice(key string) (strategicpatch.LookupPatchMeta, strategicpatch.PatchMeta, error) {
	child, info, err := m.LookupPatchMeta.LookupPatchMetadataForSlice(key)
	if m.spec && key == "ports" && err == nil {
		info.SetPatchMergeKey("name")
	}
	return child, info, err
}
func namedServicePorts(documents ...[]byte) bool {
	for _, raw := range documents {
		var doc struct {
			Spec struct{ Ports []map[string]any }
		}
		if json.Unmarshal(raw, &doc) != nil {
			return false
		}
		seen := map[string]bool{}
		for _, port := range doc.Spec.Ports {
			name, _ := port["name"].(string)
			if name == "" || seen[name] {
				return false
			}
			seen[name] = true
		}
	}
	return true
}
