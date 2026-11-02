package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"strings"
	"testing"
)

func TestUnexplainedRemovalOmissionVisible(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "app", Name: "cleanup"}
	object := urObject(ref)
	object.SetResourceVersion("")
	request := goldenRequest(t)
	request.Namespace = "app"
	store := &inventoryStore{objects: []*unstructured.Unstructured{object}}
	controller := &Controller{config: ControllerConfig{Namespace: "app", SourceCommit: request.SourceCommit}, currentIncidentRequest: &request, sourceInventory: NewSourceInventory("app", request.SourceCommit, nil), drift: &DriftControllerConfig{Inventory: true, Events: store}}
	_, request.Findings = controller.verifySourceInventoryLocked(nil)
	loop := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, 5)
	loop.MenuConfig = productionResponderMenu()
	loop.MenuConfig.Unexplained = func(context.Context) (UnexplainedRemovalAssessment, error) {
		return controller.unexplainedRemovalAssessment(request.IncidentID, "control")
	}
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu"})
	raw, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"reason":"missing_object_identity"`) {
		t.Fatalf("missing typed visible reason for omitted finding: %s", raw)
	}
}

// Kubernetes boundary only: typed fake API storage/watch notifications are shared
// by executor reads/deletes and the production informers used for closure.
type informerRemovalAPI struct {
	*lifecycleTestClient
	Client kubernetes.Interface
}

func (c informerRemovalAPI) Get(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	var object any
	var err error
	switch ref.Kind {
	case "ServiceAccount":
		object, err = c.Client.CoreV1().ServiceAccounts(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "ClusterRole":
		object, err = c.Client.RbacV1().ClusterRoles().Get(ctx, ref.Name, metav1.GetOptions{})
	case "ClusterRoleBinding":
		object, err = c.Client.RbacV1().ClusterRoleBindings().Get(ctx, ref.Name, metav1.GetOptions{})
	default:
		return nil, fmt.Errorf("unsupported synthetic resource %s", ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	raw, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: raw}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	return result, nil
}

func (c informerRemovalAPI) Delete(ctx context.Context, ref sdk.ObjectRef, uid types.UID, rv string) error {
	object, err := c.Get(ctx, ref)
	if err != nil {
		return err
	}
	if object.GetUID() != uid || object.GetResourceVersion() != rv {
		return fmt.Errorf("conditional removal identity changed")
	}
	options := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}}
	switch ref.Kind {
	case "ServiceAccount":
		return c.Client.CoreV1().ServiceAccounts(ref.Namespace).Delete(ctx, ref.Name, options)
	case "ClusterRole":
		return c.Client.RbacV1().ClusterRoles().Delete(ctx, ref.Name, options)
	case "ClusterRoleBinding":
		return c.Client.RbacV1().ClusterRoleBindings().Delete(ctx, ref.Name, options)
	}
	return fmt.Errorf("unsupported synthetic resource %s", ref.Kind)
}
