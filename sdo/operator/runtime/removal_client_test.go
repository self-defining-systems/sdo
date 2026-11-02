package runtime

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"testing"
)

func TestRemovalRechecksLiveSharedOwnershipBeforeDelete(t *testing.T) {
	for _, mode := range []string{"gate2", "new-shared-binding", "mixed-subject", "list-unavailable"} {
		t.Run(mode, func(t *testing.T) {
			objects := latentInventoryObjects()[3:]
			role, binding := objects[0], objects[1]
			for _, object := range objects {
				object.SetUID("uid")
				object.SetResourceVersion("1")
			}
			store := &inventoryStore{objects: objects}
			controller := &Controller{config: ControllerConfig{Namespace: "app", SourceCommit: "accepted"}, currentIncidentRequest: &IncidentRequest{IncidentID: "incident"}, sourceInventory: NewSourceInventory("app", "accepted", nil), drift: &DriftControllerConfig{Inventory: true, Events: store}}
			options, err := controller.unexplainedRemovals("incident", "control")
			if err != nil {
				t.Fatal(err)
			}
			var selected UnexplainedRemoval
			for _, option := range options {
				if option.Ref.Kind == "ClusterRole" {
					selected = option
				}
			}
			if selected.Ref.Name == "" {
				t.Fatal("Gate 2 role was not offered")
			}
			live := []kruntime.Object{role.DeepCopy(), binding.DeepCopy()}
			if mode == "new-shared-binding" {
				other := binding.DeepCopy()
				other.SetName("other")
				other.Object["subjects"] = []any{map[string]any{"kind": "User", "name": "outside"}}
				live = append(live, other)
			}
			if mode == "mixed-subject" {
				live[1].(*unstructured.Unstructured).Object["subjects"] = []any{map[string]any{"kind": "User", "name": "outside"}}
			}
			reader := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), map[schema.GroupVersionResource]string{
				{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
				{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
				{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			}, live...)
			if mode == "list-unavailable" {
				reader.PrependReactor("list", "*", func(ktesting.Action) (bool, kruntime.Object, error) { return true, nil, fmt.Errorf("list unavailable") })
			}
			writer := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{selected.Ref: role.DeepCopy()}}}
			client := ownershipCheckingRemovalClient{PlanLifecycleClient: writer, Reader: reader, Namespace: "app"}
			err = controller.removeUnexplained(context.Background(), "incident", "control", selected, client, selected.Ref, selected.UID, selected.ResourceVersion)
			if (err == nil) != (mode == "gate2") {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			_, present := writer.objects[selected.Ref]
			if present == (mode == "gate2") {
				t.Fatal("unsafe removal or Gate 2 deletion failed")
			}
		})
	}
}

