package sdk

// PodAdmissionResult freezes one server-side dry-run Pod create. Rejection
// contains an API failure; Error means the observation itself was unavailable.
// No Pod is persisted and no referenced Secret is read by the observer.
type PodAdmissionResult struct {
	Namespace string `json:"namespace"`
	SourcePod string `json:"source_pod,omitempty"`
	Admitted  bool   `json:"admitted"`
	Rejection string `json:"rejection,omitempty"`
	Error     string `json:"error,omitempty"`
}

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
