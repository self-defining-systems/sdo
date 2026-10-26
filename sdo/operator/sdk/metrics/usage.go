package metrics

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/sdk"
)

type UsageSource interface {
	PodUsage(sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error)
	NodeUsage(sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error)
}

// UsageUnavailableError means metrics could not be observed, never zero usage.
type UsageUnavailableError struct {
	Reason string `json:"reason"`
}

func (e *UsageUnavailableError) Error() string {
	return fmt.Sprintf("resource usage unavailable: %s", e.Reason)
}

func validateUsage(ref sdk.ObjectRef, kind string, meta metav1.ObjectMeta, timestamp metav1.Time, window metav1.Duration) error {
	if ref.Kind != kind || ref.Name == "" || (ref.APIVersion != "" && ref.APIVersion != "v1") || meta.Name != ref.Name || meta.Namespace != ref.Namespace || timestamp.IsZero() || window.Duration <= 0 || (kind == "Pod" && ref.Namespace == "") || (kind == "Node" && ref.Namespace != "") {
		return &UsageUnavailableError{Reason: kind + " metrics lack matching identity, timestamp or positive window"}
	}
	return nil
}
func validateUsageQuantities(values corev1.ResourceList) error {
	cpu, cpuOK := values[corev1.ResourceCPU]
	memory, memoryOK := values[corev1.ResourceMemory]
	if !cpuOK || !memoryOK || cpu.Sign() < 0 || memory.Sign() < 0 {
		return &UsageUnavailableError{Reason: "metrics lack nonnegative CPU and memory quantities"}
	}
	return nil
}

// ValidatePodUsage rejects incomplete observations, including recorded evidence.
func ValidatePodUsage(ref sdk.ObjectRef, sample *metricsv1beta1.PodMetrics) error {
	if sample == nil {
		return &UsageUnavailableError{Reason: "Pod metrics sample is missing"}
	}
	if err := validateUsage(ref, "Pod", sample.ObjectMeta, sample.Timestamp, sample.Window); err != nil {
		return err
	}
	if len(sample.Containers) == 0 {
		return &UsageUnavailableError{Reason: "Pod metrics contain no containers"}
	}
	seen := map[string]bool{}
	for _, c := range sample.Containers {
		if c.Name == "" || seen[c.Name] {
			return &UsageUnavailableError{Reason: "Pod metrics require unique container identities"}
		}
		seen[c.Name] = true
		if err := validateUsageQuantities(c.Usage); err != nil {
			return err
		}
	}
	return nil
}

// ValidateNodeUsage rejects incomplete observations, including recorded evidence.
func ValidateNodeUsage(ref sdk.ObjectRef, sample *metricsv1beta1.NodeMetrics) error {
	if sample == nil {
		return &UsageUnavailableError{Reason: "Node metrics sample is missing"}
	}
	if err := validateUsage(ref, "Node", sample.ObjectMeta, sample.Timestamp, sample.Window); err != nil {
		return err
	}
	return validateUsageQuantities(sample.Usage)
}
