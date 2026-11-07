package rendered

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"sdo.dev/operator/sdk"
)

// Autoscaled is the set of workloads an autoscaler targets. An autoscaler owns
// the target's spec.replicas, so SDO must not apply that field to a workload
// that exists: the apply would reset the autoscaler's decision on every
// deploy and adoption (generation churn, replica flapping). The evidence is
// the autoscaler's spec.scaleTargetRef, a shape shared by the
// HorizontalPodAutoscaler and the autoscalers that follow its scale-target
// contract; no kind or name list is kept here.
type Autoscaled struct {
	targets map[autoscaleKey]bool
}

type autoscaleKey struct{ group, kind, namespace, name string }

func scaleKey(apiVersion, kind, namespace, name string) autoscaleKey {
	group := ""
	if gv, err := schema.ParseGroupVersion(apiVersion); err == nil {
		group = gv.Group
	}
	return autoscaleKey{group: group, kind: kind, namespace: namespace, name: name}
}

// Add records that an autoscaler in namespace targets the workload.
func (a *Autoscaled) Add(apiVersion, kind, namespace, name string) {
	if a.targets == nil {
		a.targets = map[autoscaleKey]bool{}
	}
	a.targets[scaleKey(apiVersion, kind, namespace, name)] = true
}

// AddObjects records the targets of every object that carries a
// spec.scaleTargetRef. defaultNamespace stands in for an autoscaler without a
// namespace of its own.
func (a *Autoscaled) AddObjects(defaultNamespace string, objects ...*unstructured.Unstructured) {
	for _, object := range objects {
		if object == nil {
			continue
		}
		target, found, _ := unstructured.NestedMap(object.Object, "spec", "scaleTargetRef")
		if !found {
			continue
		}
		kind, _ := target["kind"].(string)
		name, _ := target["name"].(string)
		if kind == "" || name == "" {
			continue
		}
		apiVersion, _ := target["apiVersion"].(string)
		namespace := object.GetNamespace()
		if namespace == "" {
			namespace = defaultNamespace
		}
		a.Add(apiVersion, kind, namespace, name)
	}
}

// Targets reports whether an autoscaler targets ref. The API version matches
// by group, as the autoscaler's own scale-target resolution does.
func (a Autoscaled) Targets(ref sdk.ObjectRef) bool {
	return a.targets[scaleKey(ref.APIVersion, ref.Kind, ref.Namespace, ref.Name)]
}

// OmitReplicas removes spec.replicas from object when an autoscaler targets
// it, and reports whether it did. The caller keeps the field for an object that
// does not exist yet: the initial replica count is the only value SDO owns.
func (a Autoscaled) OmitReplicas(object *unstructured.Unstructured) bool {
	if object == nil || !a.Targets(sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}) {
		return false
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(object.Object, "spec", "replicas"); !found {
		return false
	}
	unstructured.RemoveNestedField(object.Object, "spec", "replicas")
	return true
}
