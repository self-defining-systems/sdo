package rendered

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
)

// AcceptedManifestRefs reads only the controller-owned render lock's identities.
// Callers must filter kinds before requesting any object content. This is not
// sufficient evidence of absence: mutations still use AcceptedManifest or
// AcceptedScopedManifest to distinguish omissions from authoritative absence.
func (s Source) AcceptedManifestRefs(ctx context.Context, revision, namespace string) ([]sdk.ObjectRef, error) {
	snapshot, err := s.readLock(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	refs := make([]sdk.ObjectRef, 0, len(snapshot.Lock.Objects))
	seen := map[sdk.ObjectRef]bool{}
	for _, entry := range snapshot.Lock.Objects {
		ref := entry.Ref
		if (sdk.ObservationTarget{Kind: ref.Kind}).Cluster() {
			ref.Namespace = ""
		}
		if seen[ref] {
			return nil, fmt.Errorf("%w: duplicate rendered identity", ErrEvidenceGap)
		}
		seen[ref] = true
		refs = append(refs, ref)
	}
	return refs, nil
}
