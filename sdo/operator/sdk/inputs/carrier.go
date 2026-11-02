// Package inputs preserves optional detector inputs across snapshot adapters.
package inputs

import (
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
)

// Sources is the complete optional input contract. New SDK input interfaces must
// be registered here and covered by the interface inventory test.
type Sources interface {
	sdk.ObservationClock
	traffic.Source
	servicehealth.ReachabilitySource
	metrics.Source
	metrics.AlertSource
	usage.Source
	logs.Source
	metrics.QuerySource
	metrics.UsageSource
	sdk.ObjectCaptureSource
	sdk.ScopedObservationSource
}

// Carrier adds all optional inputs to the Kubernetes view. Its updates return a
// value copy: composing observers never nests interface-erasing wrappers.
// Sources must be frozen for the observation cycle (lazy single-load sources
// are supported). No update mutates an earlier carrier.
type Carrier struct {
	sdk.DetectionContext
	clock        sdk.ObservationClock
	traffic      traffic.Source
	reachability servicehealth.ReachabilitySource
	metrics      metrics.Source
	alerts       metrics.AlertSource
	usage        usage.Source
	logs         logs.Source
	query        metrics.QuerySource
	podUsage     metrics.UsageSource
	capture      sdk.ObjectCaptureSource
	scoped       sdk.ScopedObservationSource
}

// New adopts existing optional inputs once, or copies an existing carrier.
func New(base sdk.DetectionContext) Carrier {
	switch c := base.(type) {
	case Carrier:
		return c
	case *Carrier:
		return *c
	}
	c := Carrier{DetectionContext: base}
	c.clock, _ = base.(sdk.ObservationClock)
	c.traffic, _ = base.(traffic.Source)
	c.reachability, _ = base.(servicehealth.ReachabilitySource)
	c.metrics, _ = base.(metrics.Source)
	c.alerts, _ = base.(metrics.AlertSource)
	c.usage, _ = base.(usage.Source)
	c.logs, _ = base.(logs.Source)
	c.query, _ = base.(metrics.QuerySource)
	c.podUsage, _ = base.(metrics.UsageSource)
	c.capture, _ = base.(sdk.ObjectCaptureSource)
	c.scoped, _ = base.(sdk.ScopedObservationSource)
	return c
}
func (c Carrier) WithClock(source sdk.ObservationClock) Carrier { c.clock = source; return c }
func (c Carrier) ObservedAt() time.Time {
	if c.clock == nil {
		return time.Time{}
	}
	return c.clock.ObservedAt()
}
func (c Carrier) WithTraffic(source traffic.Source) Carrier { c.traffic = source; return c }
func (c Carrier) TrafficWindow(id string) (traffic.Window, bool) {
	if c.traffic == nil {
		return traffic.Window{}, false
	}
	return c.traffic.TrafficWindow(id)
}
func (c Carrier) WithReachability(source servicehealth.ReachabilitySource) Carrier {
	c.reachability = source
	return c
}
func (c Carrier) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if c.reachability == nil {
		return servicehealth.ReachabilityWindow{}, false
	}
	return c.reachability.ServiceReachability()
}
func (c Carrier) WithMetrics(source metrics.Source) Carrier { c.metrics = source; return c }
func (c Carrier) MetricResult(id string) (metrics.Result, bool) {
	if c.metrics == nil {
		return metrics.Result{}, false
	}
	return c.metrics.MetricResult(id)
}
func (c Carrier) WithAlerts(source metrics.AlertSource) Carrier { c.alerts = source; return c }
func (c Carrier) AlertsResult() (metrics.AlertsResult, bool) {
	if c.alerts == nil {
		return metrics.AlertsResult{}, false
	}
	return c.alerts.AlertsResult()
}
func (c Carrier) WithUsage(source usage.Source) Carrier { c.usage = source; return c }
func (c Carrier) UsageResult() (usage.Result, bool) {
	if c.usage == nil {
		return usage.Result{}, false
	}
	return c.usage.UsageResult()
}
func (c Carrier) WithLogs(source logs.Source) Carrier { c.logs = source; return c }
func (c Carrier) LogResult(id string) (logs.Result, bool) {
	if c.logs == nil {
		return logs.Result{}, false
	}
	return c.logs.LogResult(id)
}

func (c Carrier) WithQuery(source metrics.QuerySource) Carrier { c.query = source; return c }
func (c Carrier) QueryMetrics(q metrics.Query) metrics.Result {
	if c.query == nil {
		return metrics.Result{Query: q, Error: "metric query unavailable"}
	}
	return c.query.QueryMetrics(q)
}
func (c Carrier) WithPodUsage(source metrics.UsageSource) Carrier { c.podUsage = source; return c }
func (c Carrier) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	if c.podUsage == nil {
		return nil, &metrics.UsageUnavailableError{Reason: "usage source unavailable"}
	}
	return c.podUsage.PodUsage(ref)
}
func (c Carrier) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	if c.podUsage == nil {
		return nil, &metrics.UsageUnavailableError{Reason: "usage source unavailable"}
	}
	return c.podUsage.NodeUsage(ref)
}
func (c Carrier) WithCapture(source sdk.ObjectCaptureSource) Carrier { c.capture = source; return c }
func (c Carrier) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if c.capture == nil {
		return sdk.TypedObjects(c.DetectionContext), nil
	}
	return c.capture.CaptureObjects(refs)
}
func (c Carrier) WithScoped(source sdk.ScopedObservationSource) Carrier { c.scoped = source; return c }
func (c Carrier) source() sdk.ScopedObservationSource {
	if c.scoped == nil {
		return sdk.ScopedSource(nil)
	}
	return c.scoped
}
func (c Carrier) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	return c.source().ScopedDeployments(namespace)
}
func (c Carrier) ScopedServices(namespace string) ([]corev1.Service, error) {
	return c.source().ScopedServices(namespace)
}
func (c Carrier) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	return c.source().ScopedEndpoints(namespace)
}
func (c Carrier) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	return c.source().MutatingWebhookConfigurations()
}
func (c Carrier) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	return c.source().ValidatingWebhookConfigurations()
}
func (c Carrier) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	return c.source().MutatingWebhookConfiguration(name)
}
func (c Carrier) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	return c.source().ValidatingWebhookConfiguration(name)
}
func (c Carrier) ObservationStatuses() []sdk.ObservationStatus {
	return c.source().ObservationStatuses()
}
func (c Carrier) ObservationGeneration() string { return c.source().ObservationGeneration() }

var _ Sources = Carrier{}
