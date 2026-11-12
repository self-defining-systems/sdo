package sdk

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
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
	// DataChangedAt is when a manager other than SDO last wrote the Secret's
	// data, from metadata only: the latest managedFields time of a non-SDO
	// manager that owns data fields. Creation time stands in when the object
	// records no field ownership at all, and for data owned by the API
	// server's before-first-apply entry. Zero when no foreign manager owns its
	// data (SDO alone wrote it); the stale-reader rules then stay silent.
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

// Field managers through which SDO writes application objects. Their writes
// are SDO's own and never count as a Secret data change: adopting an app
// server-side applies every accepted Secret with identical values, which adds
// an SDO entry with a new time while the data stays as the app wrote it.
const (
	DeployerFieldManager        = "sdo-deployer"
	AcceptanceFieldManager      = "sdo-acceptance"
	ReplicasHandoffFieldManager = "sdo-replicas-handoff"
	// ResponderFieldManager is forced by the recording API proxy on every write a
	// responder Job makes, whatever client sent it (`sdo act` also stamps it).
	ResponderFieldManager = "sdo-responder"
)

// BeforeFirstApplyFieldManager is the Kubernetes API server's name for the
// fields an object already had when its first server-side apply arrived. The
// entry is stamped with that apply's time, so it says nothing about when
// those fields were written.
const BeforeFirstApplyFieldManager = "before-first-apply"

// SDOFieldManagers lists every field manager SDO writes application objects as.
func SDOFieldManagers() []string {
	return []string{DeployerFieldManager, AcceptanceFieldManager, ReplicasHandoffFieldManager, ResponderFieldManager}
}

// IsSDOFieldManager reports whether manager is one of SDO's own.
func IsSDOFieldManager(manager string) bool {
	return slices.Contains(SDOFieldManagers(), manager)
}

// SecretDataChangedAt derives when a manager other than SDO last wrote a
// Secret's data from its object metadata alone. managedFields list the field
// paths each manager owns and when it last changed the object, never values;
// a manager that owns only labels or annotations does not move the time, and
// SDO's own managers never do. Data a before-first-apply entry owns dates from
// creation, the only bound metadata gives for it.
func SecretDataChangedAt(meta metav1.ObjectMeta) time.Time {
	if len(meta.ManagedFields) == 0 {
		return meta.CreationTimestamp.UTC()
	}
	var latest time.Time
	for _, entry := range meta.ManagedFields {
		if entry.Time == nil || entry.FieldsV1 == nil || IsSDOFieldManager(entry.Manager) || !ownsSecretData(entry.FieldsV1.Raw) {
			continue
		}
		at := entry.Time.Time
		if entry.Manager == BeforeFirstApplyFieldManager {
			at = meta.CreationTimestamp.Time
		}
		if at.After(latest) {
			latest = at
		}
	}
	if latest.IsZero() {
		return time.Time{}
	}
	return latest.UTC()
}

// ownsSecretData reports whether a managedFields entry owns at least one data
// key. Owning only the empty data map (left behind when another manager takes
// every key) is not a claim on any value.
func ownsSecretData(raw []byte) bool {
	var fields map[string]map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	for _, set := range []string{"f:data", "f:stringData"} {
		for key := range fields[set] {
			if strings.HasPrefix(key, "f:") {
				return true
			}
		}
	}
	return false
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
