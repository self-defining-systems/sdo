package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestPlanCredentialDeltaRefusesDroppedTokenDenial(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false}}})
	after := before.DeepCopy()
	unstructured.RemoveNestedField(after.Object, "spec", "template", "spec", "automountServiceAccountToken")
	for _, action := range []program.Action{program.ReconcileFromSource{Ref: ref}, program.Rollback{Ref: ref, ToRevision: "previous"}} {
		e := PlanExecutor{Source: planFakeSource{source: after, revision: after}}
		if _, err := e.prepareAction(context.Background(), IncidentRequest{SourceCommit: "accepted"}, action, before, false); err == nil {
			t.Errorf("%T dropped explicit token denial", action)
		}
	}
}
