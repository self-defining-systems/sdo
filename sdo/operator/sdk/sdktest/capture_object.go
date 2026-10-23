package sdktest

import (
	"encoding/json"
	"errors"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProjectCapturedObject applies the controller corpus capture projection to API
// JSON. Unsupported representations carry metadata only in the caller. Secret
// values are never observations: only their key names survive.
func ProjectCapturedObject(raw []byte) ([]byte, error) {
	var header struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Items      []json.RawMessage `json:"items"`
		Metadata   json.RawMessage   `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return nil, err
	}
	if strings.HasSuffix(header.Kind, "List") {
		kind := strings.TrimSuffix(header.Kind, "List")
		probe, _ := json.Marshal(map[string]string{"kind": kind, "apiVersion": header.APIVersion})
		if _, err := ProjectCapturedObject(probe); err != nil {
			return nil, err
		}
		items := make([]json.RawMessage, 0, len(header.Items))
		for _, item := range header.Items {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(item, &fields); err != nil {
				return nil, err
			}
			fields["kind"], _ = json.Marshal(kind)
			fields["apiVersion"], _ = json.Marshal(header.APIVersion)
			b, _ := json.Marshal(fields)
			p, err := ProjectCapturedObject(b)
			if err != nil {
				return nil, err
			}
			items = append(items, p)
		}
		// List metadata (continue/resourceVersion) is useful for scope, not identity.
		var meta metav1.ListMeta
		if len(header.Metadata) > 0 {
			if err := json.Unmarshal(header.Metadata, &meta); err != nil {
				return nil, err
			}
		}
		return json.Marshal(struct {
			APIVersion string            `json:"apiVersion"`
			Kind       string            `json:"kind"`
			Metadata   metav1.ListMeta   `json:"metadata"`
			Items      []json.RawMessage `json:"items"`
		}{header.APIVersion, header.Kind, meta, items})
	}
	// Concrete typed decoding drops unknown payload fields.
	var target any
	switch header.Kind {
	case "ConfigMap":
		target = &corev1.ConfigMap{}
	case "Service":
		target = &corev1.Service{}
	case "Pod":
		target = &corev1.Pod{}
	case "Deployment":
		target = &appsv1.Deployment{}
	case "StatefulSet":
		target = &appsv1.StatefulSet{}
	case "ReplicaSet":
		target = &appsv1.ReplicaSet{}
	case "HorizontalPodAutoscaler":
		target = &autoscalingv2.HorizontalPodAutoscaler{}
	case "PodDisruptionBudget":
		target = &policyv1.PodDisruptionBudget{}
	case "Endpoints":
		target = &corev1.Endpoints{}
	case "EndpointSlice":
		target = &discoveryv1.EndpointSlice{}
	case "NetworkPolicy":
		target = &networkingv1.NetworkPolicy{}
	case "Event":
		target = &corev1.Event{}
	case "Secret":
		// Decode values as opaque JSON to also support stringData and non-base64
		// alternate Secret values without ever copying them into the projection.
		var s struct {
			metav1.TypeMeta
			Metadata   metav1.ObjectMeta          `json:"metadata"`
			Data       map[string]json.RawMessage `json:"data"`
			StringData map[string]json.RawMessage `json:"stringData"`
		}
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		compactMeta(&s.Metadata)
		keys := func(in map[string]json.RawMessage) map[string]string {
			out := make(map[string]string, len(in))
			for k := range in {
				out[k] = ""
			}
			return out
		}
		return json.Marshal(struct {
			metav1.TypeMeta
			Metadata   metav1.ObjectMeta `json:"metadata"`
			Data       map[string]string `json:"data,omitempty"`
			StringData map[string]string `json:"stringData,omitempty"`
		}{s.TypeMeta, s.Metadata, keys(s.Data), keys(s.StringData)})
	default:
		return nil, errors.New("unsupported capture representation")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return nil, err
	}
	meta := target.(metav1.Object)
	m := metav1.ObjectMeta{Annotations: meta.GetAnnotations()}
	compactMeta(&m)
	meta.SetAnnotations(m.Annotations)
	meta.SetManagedFields(nil)
	meta.SetUID("")
	meta.SetGeneration(0)
	switch o := target.(type) {
	case *corev1.Pod:
		redactPodSpec(&o.Spec)
	case *appsv1.Deployment:
		compactMeta(&o.Spec.Template.ObjectMeta)
		redactPodSpec(&o.Spec.Template.Spec)
	case *appsv1.StatefulSet:
		compactMeta(&o.Spec.Template.ObjectMeta)
		redactPodSpec(&o.Spec.Template.Spec)
	case *appsv1.ReplicaSet:
		compactMeta(&o.Spec.Template.ObjectMeta)
		redactPodSpec(&o.Spec.Template.Spec)
	case *corev1.Event:
		o.InvolvedObject.ResourceVersion = ""
		o.InvolvedObject.UID = ""
	}
	return json.Marshal(target)
}
