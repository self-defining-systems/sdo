package runtime

import (
	"context"
	"encoding/json"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	"sdo.dev/operator/sdk/program"
)

type recordedRawClient struct{ *planFakeClient }

func newRecordedRawClient(object *unstructured.Unstructured) (recordedRawClient, sdk.ObjectRef) {
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	object.SetUID("uid")
	object.SetResourceVersion("1")
	return recordedRawClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}, ref
}
func (c recordedRawClient) Patch(ctx context.Context, ref sdk.ObjectRef, kind types.PatchType, raw []byte) error {
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	if metadata, ok := body["metadata"].(map[string]any); ok {
		current := c.objects[ref]
		if rv, ok := metadata["resourceVersion"]; ok && rv != current.GetResourceVersion() {
			return apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, ref.Name, errors.New("resourceVersion changed"))
		}
	}
	if err := c.planFakeClient.Patch(ctx, ref, kind, raw); err != nil {
		return err
	}
	object := c.objects[ref]
	rv, _ := strconv.Atoi(object.GetResourceVersion())
	object.SetResourceVersion(strconv.Itoa(rv + 1))
	return nil
}

func (c recordedRawClient) KubernetesRequest(ctx context.Context, q program.KubernetesRequest) (json.RawMessage, error) {
	if err := c.Patch(ctx, q.Ref, types.MergePatchType, q.Body); err != nil {
		return nil, err
	}
	o, err := c.Get(ctx, q.Ref)
	if err != nil {
		return nil, err
	}
	return json.Marshal(o.Object)
}
func TestRecordedRawPatchUndoRestoresAndRefusesInterveningChanges(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "restore", true: "conflict"}[changed], func(t *testing.T) {
			o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "app"}, "data": map[string]any{"value": "before"}}}
			c, ref := newRecordedRawClient(o)
			e := PlanExecutor{Client: c, Verifier: generalHealthy{}, Rollouts: generalNoRollouts{}}
			q := program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/api/v1/namespaces/app/configmaps/settings", ContentType: string(types.MergePatchType), Body: []byte(`{"data":{"value":"after"}}`), Reason: "fixture"}
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{q}}, false)
			if err != nil {
				t.Fatal(err)
			}
			if changed {
				live, _ := c.Get(context.Background(), ref)
				live.SetLabels(map[string]string{"peer": "changed"})
				c.objects[ref] = live
			}
			err = e.Rollback(context.Background(), result)
			if changed {
				if err == nil {
					t.Fatal("undo overwrote an intervening configuration change")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, _ := c.Get(context.Background(), ref)
			v, _, _ := unstructured.NestedString(got.Object, "data", "value")
			if v != "before" {
				t.Fatal(v)
			}
		})
	}

}

func TestRecordedKubernetesDryRunNeverClaimsApplied(t *testing.T) {
	o := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "app"}, "data": map[string]any{"value": "before"}}}
	raw, ref := newRecordedRawClient(o)
	client := dryRunRawClient{raw}
	e := PlanExecutor{Client: client}
	result, err := e.generalStep(context.Background(), IncidentRequest{}, program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/api/v1/namespaces/app/configmaps/settings", Query: map[string]string{"dryRun": "All"}, Body: []byte(`{"data":{"value":"preview"}}`), Reason: "validate"}, false)
	if err != nil || result.Applied || len(result.ActionsApplied) != 0 || len(result.Patches) != 0 || len(result.ActionReports) != 1 || result.ActionReports[0].Status != "validated" {
		t.Fatalf("dry-run granted mutation credit: %+v %v", result, err)
	}
}

type dryRunRawClient struct{ recordedRawClient }

func (c dryRunRawClient) KubernetesRequest(_ context.Context, q program.KubernetesRequest) (json.RawMessage, error) {
	return json.RawMessage(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"settings","namespace":"app"},"data":{"value":"preview"}}`), nil
}
