package rendered

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// InventoryEvidenceGap describes content accepted source names but does not
// declare, such as a referenced ConfigMap's data.
type InventoryEvidenceGap struct {
	Kind   string        `json:"kind"`
	Ref    sdk.ObjectRef `json:"ref"`
	Reason string        `json:"reason"`
}

func (g InventoryEvidenceGap) Error() string {
	return fmt.Sprintf("%s: %s/%s: %s", g.Kind, g.Ref.Kind, g.Ref.Name, g.Reason)
}

// Inventory contains accepted source objects for presence/ownership and RBAC
// checks only. Image fields are exactly as source writes them;
// field comparison must still use AcceptedManifest/AcceptedManifests.
type Inventory struct {
	Objects      []*unstructured.Unstructured
	EvidenceGaps []InventoryEvidenceGap
}

// AcceptedInventory lists the accepted source objects, Secrets excluded.
func (s Source) AcceptedInventory(ctx context.Context, revision, namespace string) (Inventory, error) {
	result := Inventory{}
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return result, err
	}
	for _, object := range raw {
		if strings.EqualFold(object.GetKind(), "Secret") {
			continue
		}
		result.Objects = append(result.Objects, object)
	}
	return result, nil
}
