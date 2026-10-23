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

func TestPlanCompensationRestoresRemovedCredentialFromExactPreimage(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "api", "env": []any{map[string]any{"name": "MODE", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "placeholder", "key": "placeholder"}}}}}}}}})
	before.SetUID("fixture-uid")
	before.SetResourceVersion("1")
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
	e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{false}}
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].env[0]", Value: map[string]any{"name": "MODE", "value": "safe"}}}}, false)
	if err != nil || result.VerificationPassed {
		t.Fatalf("credential removal failed: %v", err)
	}
	if err := e.Rollback(context.Background(), result); err != nil {
		t.Fatalf("exact pre-image compensation refused: %v", err)
	}
	if !planValuesEqual(client.objects[ref].Object["spec"], before.Object["spec"]) {
		t.Fatal("pre-image was not restored")
	}
	// A concurrent new channel absent from the pre-image cannot piggyback on
	// compensation, even if the owned field still holds the repair's value.
	live := result.Patches[0].After.DeepCopy()
	live.Object["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["imagePullSecrets"] = []any{map[string]any{"name": "unapproved-placeholder"}}
	client.objects[ref] = live
	if err := e.Rollback(context.Background(), result); err == nil {
		t.Fatal("compensation accepted a channel outside the captured pre-image")
	}
}
