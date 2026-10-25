package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"net/http"
	"net/http/httptest"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestGenericRawDeleteCannotReachRemovalIdentity(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "DELETE" || r.URL.Path != "/apis/rbac.authorization.k8s.io/v1/clusterroles/unrelated" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Impersonate-User") != "system:serviceaccount:control:sdo-unexplained-removal" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","status":"Failure","reason":"Forbidden","code":403}`)
			return
		}
		var options metav1.DeleteOptions
		if json.NewDecoder(r.Body).Decode(&options) != nil || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "uid" {
			t.Error("conditional identity missing")
		}
		deleted = true
		fmt.Fprint(w, `{"kind":"Status","status":"Success"}`)
	}))
	defer server.Close()
	config := &rest.Config{Host: server.URL}
	fakeClient := fake.NewSimpleClientset()
	fakeClient.Resources = []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterroles", Kind: "ClusterRole", Verbs: []string{"get", "delete"}}}}}
	removal, err := newUnexplainedRemovalClient(config, &core.GenericCapture{Namespace: "app", Discovery: fakeClient.Discovery()}, "control")
	if err != nil {
		t.Fatal(err)
	}
	generic := TypedPlanClient{ExecConfig: config}
	request := program.KubernetesRequest{Method: "DELETE", Path: "/apis/rbac.authorization.k8s.io/v1/clusterroles/unrelated"}
	if _, err = generic.KubernetesRequest(context.Background(), request); !apierrors.IsForbidden(err) || deleted {
		t.Fatalf("generic raw DELETE gained authority: %v", err)
	}
	// The dedicated client is a different authenticated transport. Production only
	// invokes it after removeUnexplained has checked the current selection.
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "unrelated"}
	if err = removal.Delete(context.Background(), ref, "uid", "1"); err != nil || !deleted {
		t.Fatalf("dedicated delete failed: %v", err)
	}
	deleted = false
	if _, err = generic.KubernetesRequest(context.Background(), request); !apierrors.IsForbidden(err) || deleted {
		t.Fatalf("identity leaked back to generic client: %v", err)
	}
	if config.Impersonate.UserName != "" {
		t.Fatal("generic config mutated")
	}
}

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
