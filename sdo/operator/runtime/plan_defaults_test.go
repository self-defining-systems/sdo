package runtime

import (
	"context"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

type revisionAnnotationClient struct{ *planFakeClient }

func (c revisionAnnotationClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	o, err := c.planFakeClient.Get(ctx, ref)
	if err == nil && len(c.patches) > 0 {
		o.SetAnnotations(map[string]string{"deployment.kubernetes.io/revision": "2"})
	}
	return o, err
}
func TestPlanDeploymentRevisionAnnotationVerificationAndCompensation(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	before := planObject(ref, map[string]any{"template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "api", "args": []any{"before"}}}}}})
	before.SetUID("u")
	before.SetResourceVersion("1")
	after := before.DeepCopy()
	_ = unstructured.SetNestedSlice(after.Object, []any{map[string]any{"name": "api", "args": []any{"after"}}}, "spec", "template", "spec", "containers")
	client := revisionAnnotationClient{&planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}}
	e := PlanExecutor{Client: client, Safety: fixturePlanSafety(t, ref, before, after), AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	proof := e.Safety.Objects[ref]
	proof.UID = "u"
	e.Safety.Objects[ref] = proof
	result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.template.spec.containers[name=api].args", Value: testPlanValue([]string{"after"})}}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("revision annotation broke verification: %v", err)
	}
	if err := e.Rollback(context.Background(), result); err != nil {
		t.Fatalf("revision annotation broke compensation: %v", err)
	}
}
