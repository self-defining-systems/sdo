package metrics

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	"sdo.dev/operator/sdk"
	"time"
)

// Usage is a metrics.k8s.io sample; an empty Container identifies a Node.
type Usage struct {
	Resource  sdk.ObjectRef     `json:"resource"`
	Container string            `json:"container,omitempty"`
	CPU       resource.Quantity `json:"cpu"`
	Memory    resource.Quantity `json:"memory"`
	Timestamp time.Time         `json:"timestamp"`
	Window    time.Duration     `json:"window"`
}

type UsageSource interface {
	PodUsage(sdk.ObjectRef) ([]Usage, error)
	NodeUsage(sdk.ObjectRef) (Usage, error)
}

// UsageUnavailableError means metrics could not be observed, never zero usage.
type UsageUnavailableError struct{ Reason string }

func (e *UsageUnavailableError) Error() string {
	return fmt.Sprintf("resource usage unavailable: %s", e.Reason)
}
