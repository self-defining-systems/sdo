package runtime

import (
	"context"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestNamedEnvironmentInsertionExecutesAndRollsBack(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "api", "env": []any{map[string]any{"name": "EXISTING", "value": "keep"}}}}}}})
	before.SetUID("fixture-uid")
	after := before.DeepCopy()
	path := "spec.template.spec.containers[name=api].env[name=DEPENDENCY_ADDR]"
	value := map[string]any{"name": "DEPENDENCY_ADDR", "value": "backend:8080"}
	if err := program.ApplySetField(after.Object, path, value); err != nil {
		t.Fatal(err)
	}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before.DeepCopy()}}
	executor := PlanExecutor{Client: client, Source: planFakeSource{source: after}, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions", SourceCommit: "accepted"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, path)}}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("apply: %v %+v", err, result)
	}
	got, err := program.FieldValue(client.objects[ref].Object, path)
	if err != nil || !reflect.DeepEqual(got, value) {
		t.Fatalf("value %v: %v", got, err)
	}
	if len(result.ActionReports) != 1 || len(result.ActionReports[0].Values) != 1 || result.ActionReports[0].Values[0].Origin != program.SourceValue {
		t.Fatal("source provenance lost")
	}
	if err := executor.Rollback(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	restored, err := program.FieldValue(client.objects[ref].Object, "spec.template.spec.containers[name=api].env")
	if err != nil {
		t.Fatal(err)
	}
	want, err := program.FieldValue(before.Object, "spec.template.spec.containers[name=api].env")
	if err != nil || !reflect.DeepEqual(restored, want) {
		t.Fatalf("rollback changed unrelated entries: %v %v", restored, err)
	}
}
