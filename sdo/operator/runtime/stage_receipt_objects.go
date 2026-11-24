package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

func touchedObjects(result PlanApplicationResult) []*contracts.ObjectRef {
	var refs []*contracts.ObjectRef
	seen := map[sdk.ObjectRef]bool{}
	add := func(ref sdk.ObjectRef) {
		if ref.Name != "" && !seen[ref] {
			seen[ref] = true
			refs = append(refs, planRefProto(ref))
		}
	}
	for _, action := range result.ActionsApplied {
		if ref, err := planActionRef(action); err == nil {
			add(ref)
		}
	}
	for _, effect := range result.ObjectEffects {
		if effect.State != "not_applied" {
			add(effect.Ref)
		}
	}
	return refs
}
