package runtime

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/sdk"
	"strings"
)

// Relevance alone never grants authority over cluster-wide authorization.
// eligible, when supplied, contains only the current unexplained objects.
func exclusiveRemovalOwnership(object *unstructured.Unstructured, namespace string, objects []*unstructured.Unstructured, eligible map[sdk.ObjectRef]bool) bool {
	if object.GetKind() != "ClusterRole" && object.GetKind() != "ClusterRoleBinding" {
		return true
	}
	if object.GetKind() == "ClusterRoleBinding" {
		return applicationBinding(object, namespace)
	}
	name := object.GetName()
	if _, bootstrap := object.GetLabels()["kubernetes.io/bootstrapping"]; bootstrap {
		return false
	}
	switch name {
	case "admin", "edit", "view", "cluster-admin":
		return false
	}
	if strings.HasPrefix(name, "system:") {
		return false
	}
	// Aggregated roles can export these permissions to other roles without a binding.
	for key := range object.GetLabels() {
		if strings.HasPrefix(key, "rbac.authorization.k8s.io/aggregate-to-") {
			return false
		}
	}
	for _, other := range objects {
		if other == nil {
			continue
		}
		if other.GetKind() == "ClusterRole" {
			selectors, found, err := unstructured.NestedSlice(other.Object, "aggregationRule", "clusterRoleSelectors")
			if err != nil {
				return false
			}
			if found {
				for _, raw := range selectors {
					value, ok := raw.(map[string]any)
					if !ok {
						return false
					}
					var selector metav1.LabelSelector
					if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(value, &selector); err != nil {
						return false
					}
					match, err := metav1.LabelSelectorAsSelector(&selector)
					if err != nil || match.Matches(labels.Set(object.GetLabels())) {
						return false
					}
				}
			}
		}
		if other.GetKind() != "RoleBinding" && other.GetKind() != "ClusterRoleBinding" {
			continue
		}
		kind, _, _ := unstructured.NestedString(other.Object, "roleRef", "kind")
		role, _, _ := unstructured.NestedString(other.Object, "roleRef", "name")
		if kind != "ClusterRole" || role != name {
			continue
		}
		if !applicationBinding(other, namespace) || inventoryOwned(other) || (eligible != nil && !eligible[inventoryRef(other)]) {
			return false
		}
	}
	return true
}

func applicationBinding(object *unstructured.Unstructured, namespace string) bool {
	if object.GetKind() == "RoleBinding" && object.GetNamespace() != namespace {
		return false
	}
	subjects, found, err := unstructured.NestedSlice(object.Object, "subjects")
	if err != nil || !found || len(subjects) == 0 {
		return false
	}
	for _, raw := range subjects {
		subject, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		ns, _ := subject["namespace"].(string)
		if ns == "" && object.GetKind() == "RoleBinding" {
			ns = object.GetNamespace()
		}
		if subject["kind"] != "ServiceAccount" || ns != namespace || subject["name"] == "" {
			return false
		}
	}
	return true
}
