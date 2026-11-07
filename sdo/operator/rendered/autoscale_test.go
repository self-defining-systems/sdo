package rendered

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

func scaler(kind, namespace string, target map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling/v2", "kind": kind,
		"metadata": map[string]any{"name": "a", "namespace": namespace},
		"spec":     map[string]any{"scaleTargetRef": target},
	}}
}

func workload(namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": namespace},
		"spec":     map[string]any{"replicas": int64(3)},
	}}
}

// Any object with a scaleTargetRef is an autoscaler: no kind list is kept.
func TestAutoscaledOmitsReplicasOfEveryTargetKind(t *testing.T) {
	target := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}
	for _, kind := range []string{"HorizontalPodAutoscaler", "ScaledObject"} {
		var autoscaled Autoscaled
		autoscaled.AddObjects("app", scaler(kind, "app", target))
		object := workload("app")
		if !autoscaled.OmitReplicas(object) {
			t.Fatalf("%s: replicas kept", kind)
		}
		if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", "replicas"); found {
			t.Fatalf("%s: spec.replicas still set", kind)
		}
	}
}

func TestAutoscaledIsScopedToTheTarget(t *testing.T) {
	var autoscaled Autoscaled
	autoscaled.AddObjects("app", scaler("HorizontalPodAutoscaler", "", map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web"}))
	if !autoscaled.Targets(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}) {
		t.Error("a namespaceless autoscaler targets the default namespace")
	}
	for _, other := range []sdk.ObjectRef{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "other", Name: "web"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"},
		{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "web"},
	} {
		if autoscaled.Targets(other) {
			t.Errorf("%v is not targeted", other)
		}
	}
	if autoscaled.OmitReplicas(workload("other")) {
		t.Error("an untargeted workload keeps its replicas")
	}
}
