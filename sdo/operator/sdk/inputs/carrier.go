// Package inputs preserves optional detector inputs across snapshot adapters.
package inputs

import (
	"time"

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

var _ Sources = Carrier{}
