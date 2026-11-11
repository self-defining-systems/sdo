package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

type deleteEffectClient struct {
	PlanObjectClient
	deleted bool
	before  *unstructured.Unstructured
}

func (c *deleteEffectClient) ResolveDeleteTarget(r program.KubernetesRequest) (sdk.ObjectRef, error) {
	return r.Ref, nil
}
func (c *deleteEffectClient) Get(context.Context, sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if c.deleted {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "validatingwebhookconfigurations"}, "broken")
	}
	return c.before.DeepCopy(), nil
}
func (c *deleteEffectClient) KubernetesRequest(_ context.Context, r program.KubernetesRequest) (json.RawMessage, error) {
	var options struct {
		Preconditions struct {
			UID             string `json:"uid"`
			ResourceVersion string `json:"resourceVersion"`
		} `json:"preconditions"`
	}
	if err := json.Unmarshal(r.Body, &options); err != nil {
		return nil, err
	}
	if options.Preconditions.UID != "exact-uid" || options.Preconditions.ResourceVersion != "1" {
		return nil, fmt.Errorf("delete identity is not conditional")
	}
	c.deleted = true
	return json.RawMessage(`{"status":"Success"}`), nil
}
func TestRawDeleteRetainsIndependentObjectEffectBeforeHealthVerification(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "broken"}
	before := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "uid": "exact-uid", "resourceVersion": "1"}}}
	client := &deleteEffectClient{before: before}
	executor := PlanExecutor{Client: client}
	result, err := executor.generalStep(context.Background(), IncidentRequest{}, program.KubernetesRequest{Method: "DELETE", Path: "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/broken", Ref: ref}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ObjectEffects) != 1 {
		t.Fatal("confirmed DELETE lost UID-bound object effect before independent detector verification")
	}
	effect := result.ObjectEffects[0]
	if !effect.Confirmed || effect.UID != "exact-uid" || effect.DeletedPreImage == nil || effect.Operation != "delete" {
		t.Fatalf("incomplete effect: %+v", effect)
	}
	if !(&ResponderLoop{}).recoveryPlanFieldsMatch(result, func(sdk.ObjectRef) *unstructured.Unstructured { return nil }, func(sdk.ObjectRef) bool { return true }) {
		t.Fatal("confirmed absent delete cannot settle")
	}
}
