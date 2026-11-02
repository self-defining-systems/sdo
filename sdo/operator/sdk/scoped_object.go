package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// ProjectObservationObject uses exactly the SDK projection for source and drift.
func ProjectObservationObject(object *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	if object == nil {
		return nil, nil
	}
	data, err := json.Marshal(object.Object)
	if err != nil {
		return nil, err
	}
	var projected any
	switch object.GetKind() {
	case "Deployment":
		var o appsv1.Deployment
		if err = json.Unmarshal(data, &o); err != nil {
			return nil, err
		}
		projected = ProjectForeignDeployment(o)
	case "Service":
		var o corev1.Service
		if err = json.Unmarshal(data, &o); err != nil {
			return nil, err
		}
		projected = ProjectForeignService(o)
	case "Endpoints":
		var o corev1.Endpoints
		if err = json.Unmarshal(data, &o); err != nil {
			return nil, err
		}
		projected = ProjectForeignEndpoints(o)
	case "MutatingWebhookConfiguration":
		var o admissionv1.MutatingWebhookConfiguration
		if err = json.Unmarshal(data, &o); err != nil {
			return nil, err
		}
		projected = ProjectMutatingWebhook(o)
	case "ValidatingWebhookConfiguration":
		var o admissionv1.ValidatingWebhookConfiguration
		if err = json.Unmarshal(data, &o); err != nil {
			return nil, err
		}
		projected = ProjectValidatingWebhook(o)
	default:
		return nil, fmt.Errorf("unsupported scoped projection")
	}
	data, err = json.Marshal(projected)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	out := &unstructured.Unstructured{Object: m}
	out.SetKind(object.GetKind())
	out.SetAPIVersion(object.GetAPIVersion())
	delete(out.Object, "status")
	return out, nil
}

// ErrScopedObjectAbsent reports a declared, observed scoped target that does
// not contain the named object.
var ErrScopedObjectAbsent = errors.New("object is absent from snapshot")

// ScopedObject reads one object of a declared scoped observation target. It is
// the single projection shared by live and host-side read views.
func ScopedObject(source ScopedObservationSource, ref ObjectRef) (*unstructured.Unstructured, error) {
	t := ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
	if t.Cluster() {
		t.Name = ref.Name
	}
	matched := false
	for _, status := range source.ObservationStatuses() {
		if status.Target == t {
			matched = true
			if status.State != ObservationObserved {
				return nil, errors.New("scoped observation unavailable")
			}
		}
	}
	if !matched {
		return nil, errors.New("scoped observation undeclared")
	}
	var object any
	switch ref.Kind {
	case "Deployment":
		items, err := source.ScopedDeployments(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "Service":
		items, err := source.ScopedServices(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "Endpoints":
		items, err := source.ScopedEndpoints(ref.Namespace)
		if err != nil {
			return nil, err
		}
		for _, o := range items {
			if o.Name == ref.Name {
				object = o
			}
		}
	case "MutatingWebhookConfiguration":
		item, err := source.MutatingWebhookConfiguration(ref.Name)
		if err != nil {
			return nil, err
		}
		if item != nil {
			object = item
		}
	case "ValidatingWebhookConfiguration":
		item, err := source.ValidatingWebhookConfiguration(ref.Name)
		if err != nil {
			return nil, err
		}
		if item != nil {
			object = item
		}
	}
	if object == nil {
		return nil, ErrScopedObjectAbsent
	}
	b, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	out := &unstructured.Unstructured{Object: m}
	out.SetAPIVersion(ref.APIVersion)
	out.SetKind(ref.Kind)
	delete(out.Object, "status")
	return out, nil
}
