package program

import (
	"encoding/json"
	"fmt"

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
