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

// recoveryOutageClient models an API outage independently of the controller's
// journal. A lost receipt commits the write, then hides its first readback.
type recoveryOutageClient struct {
	*planFakeClient
	objectClient  PlanObjectClient
	attempts      int
	rejectAttempt int
	loseReceipt   bool
	rejectRead    bool
}

func (c *recoveryOutageClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, patch []byte) error {
	c.attempts++
	if c.attempts == c.rejectAttempt {
		return errors.New("synthetic write outage")
	}
	backend := c.objectClient
	if backend == nil {
		backend = c.planFakeClient
	}
	if err := backend.Patch(ctx, ref, kind, patch); err != nil {
		return err
	}
	if c.loseReceipt {
		c.loseReceipt = false
		c.rejectRead = true
		return errors.New("synthetic receipt loss")
	}
	return nil
}
func (c *recoveryOutageClient) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.rejectRead {
		c.rejectRead = false
		return nil, errors.New("synthetic read outage")
	}
	if c.objectClient != nil {
		return c.objectClient.Get(ctx, ref)
	}
	return c.planFakeClient.Get(ctx, ref)
}

type recoveryRolloutFailure struct{}

func (recoveryRolloutFailure) Wait(context.Context, []sdk.ObjectRef) error {
	return errors.New("synthetic rollout failure")
}

func repeatedReplicaPlan() (sdk.ObjectRef, program.Plan, *recoveryOutageClient) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "workers"}
	client := &recoveryOutageClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"replicas": float64(1)})}}}
	plan := program.Plan{}
	for _, value := range []int{2, 3, 4} {
		plan.Actions = append(plan.Actions, program.SetField{Ref: ref, Path: "spec.replicas", Value: testPlanValue(value)})
	}
	return ref, plan, client
}
func requireRecoveredReplicas(t *testing.T, client *recoveryOutageClient, ref sdk.ObjectRef) {
	t.Helper()
	value, _ := testField(client.objects[ref], "spec.replicas")
	if value != float64(1) {
		t.Fatalf("compensation left replicas=%v; want 1", value)
	}
}
