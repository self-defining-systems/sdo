package sdk

import (
	"encoding/json"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SecretMetadata exposes only identity, type, labels, data key names and
// when the data last changed. It cannot hold Secret values.
type SecretMetadata struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Type      corev1.SecretType `json:"type"`
	Labels    map[string]string `json:"labels,omitempty"`
	DataKeys  []string          `json:"dataKeys"`
	// DataChangedAt is when the Secret's data was last written, from metadata
	// only: the latest managedFields time of a manager that owns data fields,
	// else the creation time. Zero when the object records neither.
	DataChangedAt time.Time `json:"dataChangedAt,omitzero"`
}

// SecretMetadataFrom discards values, annotations and all other Secret fields.
func SecretMetadataFrom(secret corev1.Secret) SecretMetadata {
	result := SecretMetadata{Name: secret.Name, Namespace: secret.Namespace, Type: secret.Type, Labels: map[string]string{}, DataChangedAt: SecretDataChangedAt(secret.ObjectMeta)}
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

// SecretDataChangedAt derives when a Secret's data was last written from its
// object metadata alone. managedFields list the field paths each manager owns
// and when it last wrote, never values; a manager that owns only labels or
// annotations does not move the time.
func SecretDataChangedAt(meta metav1.ObjectMeta) time.Time {
	var latest time.Time
	for _, entry := range meta.ManagedFields {
		if entry.Time == nil || entry.FieldsV1 == nil || !ownsSecretData(entry.FieldsV1.Raw) {
			continue
		}
		if entry.Time.After(latest) {
			latest = entry.Time.Time
		}
	}
	if latest.IsZero() {
		latest = meta.CreationTimestamp.Time
	}
	return latest.UTC()
}

func ownsSecretData(raw []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	_, data := fields["f:data"]
	_, stringData := fields["f:stringData"]
	return data || stringData
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
