package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"testing"
)

func TestGenericPlanTransportSupportsNewKindWithoutRegistration(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "new.example/v1", Kind: "NeverRegistered", Namespace: "app", Name: "x"}
	object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"mode": "broken"}}}
	dynamic := dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme(), object)
	discovery := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: []*metav1.APIResourceList{{GroupVersion: ref.APIVersion, APIResources: []metav1.APIResource{{Kind: ref.Kind, Name: "neverregistereds", Namespaced: true, Verbs: []string{"get", "list", "patch"}}}}}}}
	client := GenericPlanClient{Objects: &core.GenericCapture{Client: dynamic, Discovery: discovery, Namespace: "app"}}
	if err := client.Patch(context.Background(), ref, types.MergePatchType, []byte(`{"spec":{"mode":"fixed"}}`)); err != nil {
		t.Fatal(err)
	}
	got, err := client.Get(context.Background(), ref)
	if err != nil || got.Object["spec"].(map[string]any)["mode"] != "fixed" {
		t.Fatalf("generic patch: %v %v", got, err)
	}
}
