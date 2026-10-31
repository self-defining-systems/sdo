package sdk

import (
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecretMetadata exposes only identity, type, labels and data key names. It cannot hold Secret values.
type SecretMetadata struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Type      corev1.SecretType `json:"type"`
	Labels    map[string]string `json:"labels,omitempty"`
	DataKeys  []string          `json:"dataKeys"`
}

// SecretMetadataFrom discards values, annotations and all other Secret fields.
func SecretMetadataFrom(secret corev1.Secret) SecretMetadata {
	result := SecretMetadata{Name: secret.Name, Namespace: secret.Namespace, Type: secret.Type, Labels: map[string]string{}}
	for key, value := range secret.Labels {
		result.Labels[key] = value
	}
	keys := map[string]bool{}
	for key := range secret.Data {
		keys[key] = true
	}
	for key := range secret.StringData {
		keys[key] = true
	}
	for key := range keys {
		result.DataKeys = append(result.DataKeys, key)
	}
	sort.Strings(result.DataKeys)
	return result
}

// DeepCopy returns an independent copy of the metadata.
func (s SecretMetadata) DeepCopy() *SecretMetadata {
	r := s
	r.Labels = map[string]string{}
	for k, v := range s.Labels {
		r.Labels[k] = v
	}
	r.DataKeys = append([]string(nil), s.DataKeys...)
	return &r
}

// ObjectMeta returns the safe Kubernetes identity for read-view projections.
func (s SecretMetadata) ObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: s.DeepCopy().Labels}
}
