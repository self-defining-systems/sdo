package rendered

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
)

// AcceptedManifestRefs lists the identities source declares. Callers must
// filter kinds before requesting any object content.
func (s Source) AcceptedManifestRefs(ctx context.Context, revision, namespace string) ([]sdk.ObjectRef, error) {
	raw, err := s.RawManifests(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	refs := make([]sdk.ObjectRef, 0, len(raw))
	seen := map[sdk.ObjectRef]bool{}
	for _, object := range raw {
		ref := objectRef(object)
		if (sdk.ObservationTarget{Kind: ref.Kind}).Cluster() {
			ref.Namespace = ""
		}
		if seen[ref] {
			return nil, fmt.Errorf("duplicate rendered identity %s/%s", ref.Kind, ref.Name)
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs, nil
}
