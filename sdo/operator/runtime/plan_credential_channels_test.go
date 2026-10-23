package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// P1: legacy numeric env[N] SetField carrying a secretKeyRef value, no Safety policy at all.
func TestRV13LegacyEnvSecretKeyRefBypass(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "app"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "env": []any{map[string]any{"name": "MODE", "value": "x"}}}}}}})
	before.SetUID("u")
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
	e := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	val := map[string]any{"name": "MODE", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "placeholder", "key": "placeholder"}}}
	a := program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].env[0]", Value: val, Reason: "probe"}
	_, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{a}}, false)
	t.Logf("apply err=%v patches=%d", err, len(client.patches))
	if err == nil && len(client.patches) > 0 {
		t.Fatalf("BYPASS: secretKeyRef env written with nil Safety; patch=%s", client.patches[0])
	}
}

// P1b: same via whole containers entry? (path restricts) and via valueFrom subpath on existing ref
func TestRV13LegacyEnvValueFromSubpath(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "app"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "env": []any{map[string]any{"name": "MODE", "valueFrom": map[string]any{"configMapKeyRef": map[string]any{"name": "cm", "key": "k"}}}}}}}}})
	before.SetUID("u")
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
	e := PlanExecutor{Client: client, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	a := program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].env[0].valueFrom", Value: map[string]any{"secretKeyRef": map[string]any{"name": "placeholder", "key": "placeholder"}}, Reason: "probe"}
	_, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{a}}, false)
	t.Logf("apply err=%v patches=%d", err, len(client.patches))
	if err == nil && len(client.patches) > 0 {
		t.Fatalf("BYPASS valueFrom: %s", client.patches[0])
	}
}

func TestPlanCredentialDeltaAllActionForms(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "app"}
	before := planObject(ref, map[string]any{"replicas": int64(1), "template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "app", "env": []any{map[string]any{"name": "MODE", "value": "x"}}}}}}})
	before.SetUID("u")
	channel := map[string]any{"name": "MODE", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "placeholder", "key": "placeholder"}}}
	source := before.DeepCopy()
	_ = unstructured.SetNestedSlice(source.Object, []any{map[string]any{"name": "app", "env": []any{channel}}}, "spec", "template", "spec", "containers")
	for _, action := range []program.Action{
		program.SetField{Ref: ref, Path: "spec.template.spec.containers[0].env[0]", Value: channel},
		program.SetField{Ref: ref, Path: "spec.template.spec.containers[name=app].env[name=MODE]", Value: channel},
		program.ReconcileFromSource{Ref: ref}, program.Rollback{Ref: ref, ToRevision: "previous"},
	} {
		// Exercise the shared post-image boundary directly too: named paths cannot
		// hide behind a missing structured authorization in this regression.
		executor := PlanExecutor{Source: planFakeSource{source: source, revision: source}}
		if _, err := executor.prepareAction(context.Background(), IncidentRequest{SourceCommit: "accepted"}, action, before, false); err == nil {
			t.Fatalf("credential addition accepted for %T", action)
		}
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: source.DeepCopy()}}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	_, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: 2}}}, false)
	if err != nil || len(client.patches) != 1 {
		t.Fatalf("unrelated legacy repair refused: %v", err)
	}
}
