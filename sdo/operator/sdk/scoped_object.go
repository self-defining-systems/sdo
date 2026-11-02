package sdk

import (
	"encoding/json"
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
