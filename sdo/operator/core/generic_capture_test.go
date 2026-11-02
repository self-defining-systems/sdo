package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

func TestDiscoveryCaptureAnyKindAndIncidentScopes(t *testing.T) {
	resources := []*metav1.APIResourceList{
		{GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "persistentvolumeclaims", Kind: "PersistentVolumeClaim", Namespaced: true, Verbs: []string{"get", "list", "patch"}}, {Name: "secrets", Kind: "Secret", Namespaced: true, Verbs: []string{"get", "list", "patch"}}}},
		{GroupVersion: "example.test/v1", APIResources: []metav1.APIResource{{Name: "widgets", Kind: "Widget", Namespaced: true, Verbs: []string{"get", "list", "patch"}}, {Name: "policies", Kind: "Policy", Verbs: []string{"get", "list", "patch"}}}},
	}
	var objects []kruntime.Object
	for _, ref := range []sdk.ObjectRef{{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "app", Name: "claim"}, {APIVersion: "example.test/v1", Kind: "Widget", Namespace: "app", Name: "new-kind"}, {APIVersion: "example.test/v1", Kind: "Widget", Namespace: "policy-system", Name: "foreign"}, {APIVersion: "example.test/v1", Kind: "Policy", Name: "cluster"}, {APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "never-read"}} {
		objects = append(objects, &unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name, "namespace": ref.Namespace}, "spec": map[string]any{"flag": "broken"}}})
	}
	fake := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), map[schema.GroupVersionResource]string{{Version: "v1", Resource: "persistentvolumeclaims"}: "PersistentVolumeClaimList", {Version: "v1", Resource: "secrets"}: "SecretList", {Group: "example.test", Version: "v1", Resource: "widgets"}: "WidgetList", {Group: "example.test", Version: "v1", Resource: "policies"}: "PolicyList"}, objects...)
	fake.PrependReactor("*", "secrets", func(clienttesting.Action) (bool, kruntime.Object, error) {
		t.Fatal("Secret read attempted")
		return true, nil, nil
	})
	discovery := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
	source := &GenericCapture{Context: context.Background(), Namespace: "app", Client: fake, Discovery: discovery}
	captured, err := source.CaptureObjects([]sdk.ObjectRef{{Kind: "Widget", Namespace: "policy-system", Name: "foreign"}, {Kind: "Policy", Name: "cluster"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(captured) != 4 {
		t.Fatalf("captured %d objects: %+v", len(captured), captured)
	}
	// Guard: every discovered mutable nonsecret resource can enter the same fixture
	// and be read with its exact version, kind, namespace and name.
	fixture, err := sdktest.CaptureSnapshot(DetectionSnapshot{NamespaceName: "app", GenericCapture: source}, "app", sdk.ObjectRef{Kind: "Widget", Namespace: "policy-system"}, sdk.ObjectRef{Kind: "Policy", Name: "cluster"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(fixture)
	if strings.Contains(string(raw), "never-read") {
		t.Fatal("Secret captured")
	}
	for _, object := range captured {
		if _, err := fixture.Object(sdk.ObjectIdentity(object)); err != nil {
			t.Fatal(err)
		}
	}
}
