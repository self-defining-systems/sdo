package runtime

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type failingBatchClient struct {
	*planFakeClient
	calls        int
	failRollback bool
}

func (c *failingBatchClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.calls++
	if c.calls == 2 || (c.failRollback && c.calls > 2) {
		return errors.New("admission rejected patch")
	}
	return c.planFakeClient.Patch(ctx, ref, kind, patch)
}
func TestPlanExecutorCompensatesSecondPatchFailure(t *testing.T) {
	for _, failRollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "compensated", true: "rollback fails"}[failRollback], func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			client := &failingBatchClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}, failRollback: failRollback}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{passed: true}}
			result, err := executor.Apply(context.Background(), IncidentRequest{RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(2)}, program.SetField{Ref: ref, Path: "spec.replicas", Value: float64(3)}}}, false)
			if err == nil || !result.Applied || len(result.ActionsApplied) != 1 {
				t.Fatalf("partial application lost: %+v, %v", result, err)
			}
			replicas, _ := testField(client.objects[ref], "spec.replicas")
			if !failRollback && replicas != float64(1) {
				t.Fatalf("partial write remains: %v", replicas)
			}
			if failRollback && replicas != float64(2) {
				t.Fatalf("unexpected failed rollback value: %v", replicas)
			}
		})
	}
}
