package sdk

import "time"

// PodAdmissionResult freezes one server-side dry-run Pod create. Rejection
// contains an API failure; Error means the observation itself was unavailable.
// No Pod is persisted and no referenced Secret is read by the observer.
//
// The dry-run Pod is built from the pod template of the source Pod's
// controller (its ReplicaSet, StatefulSet or DaemonSet) when the snapshot holds
// it, so Mutations shows what admission changes in a Pod the workload would
// create now, not what an earlier admission already wrote into a running Pod.
type PodAdmissionResult struct {
	Namespace string `json:"namespace"`
	SourcePod string `json:"source_pod,omitempty"`
	Admitted  bool   `json:"admitted"`
	Rejection string `json:"rejection,omitempty"`
	Error     string `json:"error,omitempty"`
	// Latency is how long the API server took to answer the dry-run create,
	// admission webhooks included; a webhook that hangs shows here.
	Latency time.Duration `json:"latency_ns,omitempty"`
	// Mutations are the fields admission changed in the admitted Pod compared
	// with the Pod submitted, excluding values the API server fills in by
	// default (pull policy, termination message, service account token volume,
	// default tolerations and the like). Webhooks and built-in admission such
	// as a LimitRange both mutate; Webhooks names the candidates.
	Mutations []FieldChange `json:"mutations,omitempty"`
	// Webhooks are the admission webhook entries whose rules, namespaceSelector
	// and objectSelector select the submitted Pod, so the entries that took
	// part in its admission. With Mutations and a single mutating entry, the
	// mutation is attributed exactly.
	Webhooks []AdmissionWebhook `json:"webhooks,omitempty"`
}

// MutatingWebhooks returns the selected entries of mutating configurations.
func (r PodAdmissionResult) MutatingWebhooks() []AdmissionWebhook {
	var out []AdmissionWebhook
	for _, webhook := range r.Webhooks {
		if webhook.Mutating() {
			out = append(out, webhook)
		}
	}
	return out
}

// FieldChange is one field admission changed. Path names the field with
// containers by name (spec.containers[api].resources.limits.memory); Before
// and After are JSON values, empty when the field was absent.
type FieldChange struct {
	Path   string `json:"path"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

// MaxAdmissionMutations bounds the mutations one observation records.
const MaxAdmissionMutations = 32

// ConsumesPodAdmission enables the bounded API observation only for bundles
// containing a detector that explicitly requests it.
func ConsumesPodAdmission(detectors []Detector) bool {
	for _, detector := range detectors {
		if consumer, ok := detector.(interface{ ConsumesPodAdmission() bool }); ok && consumer.ConsumesPodAdmission() {
			return true
		}
	}
	return false
}
