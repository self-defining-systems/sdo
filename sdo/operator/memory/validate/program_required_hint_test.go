package validate

import (
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A hand repair recorded on a generated Pod must be learned against the
// workload that owns it: the Pod name dies with the Pod, so a role bound to it
// never binds on the next incident.
func TestProgramRequiredHintBindsTheOwningWorkloadNotTheLivePod(t *testing.T) {
	pod := &contracts.ObjectRef{ApiVersion: "v1", Kind: "Pod", Namespace: "app", Name: "cache-7c9d8b6f45-x2lzq"}
	owned := &contracts.RepairActionReceipt{ActionId: "exec-1", Resources: []*contracts.ObjectRef{pod}, Owners: []*contracts.ResourceOwners{{Resource: pod, OwnerChain: []*contracts.ObjectRef{
		{ApiVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "cache-7c9d8b6f45"},
		{ApiVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "cache"},
	}}}}
	for _, linked := range []bool{false, true} {
		hint := programRequiredHint("inc-1", owned, linked, []bool{false})
		bind := hint
		if !linked {
			bind, _, _ = strings.Cut(hint, "append this entry")
		}
		if !strings.Contains(bind, `"kind":"Deployment"`) || strings.Contains(bind, "x2lzq") {
			t.Fatalf("linked=%v: hint must bind the owning Deployment, not the Pod: %s", linked, hint)
		}
	}
	unowned := &contracts.RepairActionReceipt{ActionId: "exec-2", Resources: []*contracts.ObjectRef{pod}}
	if hint := programRequiredHint("inc-1", unowned, true, []bool{false}); !strings.Contains(hint, "x2lzq") {
		t.Fatalf("without a recorded owner the hint names the receipt's resource: %s", hint)
	}
}
