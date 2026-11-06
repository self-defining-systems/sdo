package rendered

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

func desiredOf(objects ...map[string]any) DesiredSet {
	var phase DesiredPhase
	for _, o := range objects {
		u := &unstructured.Unstructured{Object: o}
		phase.Objects = append(phase.Objects, DesiredObject{Ref: sdk.ObjectRef{APIVersion: u.GetAPIVersion(), Kind: u.GetKind(), Namespace: u.GetNamespace(), Name: u.GetName()}, Object: u})
	}
	return DesiredSet{Phases: []DesiredPhase{phase}}
}

func object(apiVersion, kind, name string) map[string]any {
	return map[string]any{"apiVersion": apiVersion, "kind": kind, "metadata": map[string]any{"name": name, "namespace": "n"}}
}

func TestCheckServedAPIs(t *testing.T) {
	served := []string{"v1", "v1/ConfigMap", "policy/v1", "policy/v1/PodDisruptionBudget", "apiextensions.k8s.io/v1", "apiextensions.k8s.io/v1/CustomResourceDefinition"}
	crd := object("apiextensions.k8s.io/v1", "CustomResourceDefinition", "widgets.example.dev")
	crd["spec"] = map[string]any{"group": "example.dev", "names": map[string]any{"kind": "Widget"}}

	if err := CheckServedAPIs(desiredOf(object("v1", "ConfigMap", "a"), object("policy/v1", "PodDisruptionBudget", "b")), served); err != nil {
		t.Fatalf("served objects: %v", err)
	}
	if err := CheckServedAPIs(desiredOf(object("policy/v1beta1", "PodDisruptionBudget", "b")), nil); err != nil {
		t.Fatalf("unknown discovery must pass: %v", err)
	}
	err := CheckServedAPIs(desiredOf(object("v1", "ConfigMap", "a"), object("policy/v1beta1", "PodDisruptionBudget", "b")), served)
	var unserved *UnservedAPIError
	if !errors.As(err, &unserved) || len(unserved.Objects) != 1 || unserved.Objects[0] != "policy/v1beta1 PodDisruptionBudget n/b" {
		t.Fatalf("want one unserved PodDisruptionBudget, got %v", err)
	}
	if err := CheckServedAPIs(desiredOf(crd, object("example.dev/v1", "Widget", "w")), served); err != nil {
		t.Fatalf("a kind the same render's CRD defines is not unserved: %v", err)
	}
	if err := CheckServedAPIs(desiredOf(object("example.dev/v1", "Widget", "w")), served); err == nil {
		t.Fatal("a custom kind without its CRD must be reported")
	}
}
