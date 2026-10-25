package rendered

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// InventoryEvidenceGap describes unavailable rendered-field evidence. It does
// not imply that the object's accepted source identity is unavailable.
type InventoryEvidenceGap struct {
	Kind   string        `json:"kind"`
	Ref    sdk.ObjectRef `json:"ref"`
	Reason string        `json:"reason"`
}

func (g InventoryEvidenceGap) Error() string {
	return fmt.Sprintf("%s: %s/%s: %s", g.Kind, g.Ref.Kind, g.Ref.Name, g.Reason)
}

func (g InventoryEvidenceGap) Unwrap() error { return ErrEvidenceGap }

// Inventory contains accepted source objects for presence/ownership and RBAC
// checks only. Image fields are source inputs, not deployed-image evidence;
// field comparison must still use AcceptedManifest/AcceptedManifests.
type Inventory struct {
	Objects      []*unstructured.Unstructured
	EvidenceGaps []InventoryEvidenceGap
}

// AcceptedInventory requires the same accepted selection and source freshness
// as field comparison. A missing per-object render cannot hide a source-declared
// identity or its descendants, nor prevent checking unknown live objects.
func (s Source) AcceptedInventory(ctx context.Context, revision, namespace string) (Inventory, error) {
	result := Inventory{EvidenceGaps: []InventoryEvidenceGap{}}
	snapshot, err := s.readLock(ctx, revision, namespace)
	if err != nil {
		return result, err
	}
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return result, err
	}
	entries := make(map[sdk.ObjectRef]Object, len(snapshot.Lock.Objects))
	for _, entry := range snapshot.Lock.Objects {
		entries[entry.Ref] = entry
	}
	for _, object := range raw {
		if strings.EqualFold(object.GetKind(), "Secret") {
			continue
		}
		ref := objectRef(object)
		entry, exists := entries[ref]
		var evidenceErr error
		if !exists {
			evidenceErr = fmt.Errorf("missing lock object")
		} else {
			_, evidenceErr = s.lockedObject(ctx, revision, snapshot, entry)
		}
		if evidenceErr != nil {
			result.EvidenceGaps = append(result.EvidenceGaps, InventoryEvidenceGap{Kind: "rendered_object", Ref: ref, Reason: evidenceErr.Error()})
		}
		result.Objects = append(result.Objects, object)
	}
	return result, nil
}
