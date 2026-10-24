package program

import (
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	policyv1 "k8s.io/api/policy/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"time"
)

// ReadFrom exposes the shared runtime reads to a detector. An object-only
// context returns explicit unavailable results for unsupported reads.
func ReadFrom(view sdk.DetectionContext) ReadView {
	if full, ok := view.(ReadView); ok {
		return full
	}
	if source, ok := view.(interface{ RuntimeReadView() ReadView }); ok {
		return source.RuntimeReadView()
	}
	return snapshotReadView{DetectionContext: view, ScopedObservationSource: sdk.ScopedSource(view)}
}

// CheckReads gives one evaluation its own failure tracking. A failed read
// cannot be ignored to turn Unknown into clearance.
func CheckReads(view ReadView) (sdk.DetectionContext, func() error) {
	checked := &checkedReadView{ReadView: view}
	return checked, checked.failure
}
func (v snapshotReadView) MetricResult(id string) (metrics.Result, bool) {
	return metrics.ResultFrom(v.DetectionContext, id)
}
func (v snapshotReadView) AlertsResult() (metrics.AlertsResult, bool) {
	return metrics.AlertsFrom(v.DetectionContext)
}
func (v snapshotReadView) QueryMetrics(q metrics.Query) metrics.Result {
	if source, ok := v.DetectionContext.(metrics.QuerySource); ok {
		return source.QueryMetrics(q)
	}
	if result, ok := metrics.ResultFrom(v.DetectionContext, q.ID); ok && result.Query == q {
		return result
	}
	return metrics.Result{Query: q, Error: "metric query unavailable"}
}
func (v snapshotReadView) TrafficWindow(name string) (traffic.Window, bool) {
	return traffic.WindowFrom(v.DetectionContext, name)
}
func (v *checkedReadView) MetricResult(id string) (metrics.Result, bool) {
	r, ok := v.ReadView.MetricResult(id)
	if !ok {
		v.check(fmt.Errorf("metric %q unavailable", id))
	} else if r.Error != "" {
		v.check(fmt.Errorf("metric %q: %s", id, r.Error))
	}
	return r, ok
}
func (v *checkedReadView) AlertsResult() (metrics.AlertsResult, bool) {
	r, ok := v.ReadView.AlertsResult()
	if !ok {
		v.check(fmt.Errorf("alerts unavailable"))
	} else if r.Error != "" {
		v.check(fmt.Errorf("alerts: %s", r.Error))
	}
	return r, ok
}
func (v *checkedReadView) QueryMetrics(q metrics.Query) metrics.Result {
	r := v.ReadView.QueryMetrics(q)
	if r.Error != "" {
		v.check(fmt.Errorf("metric %q: %s", q.ID, r.Error))
	}
	return r
}

func (v snapshotReadView) StatefulSets() []appsv1.StatefulSet {
	if source, ok := v.DetectionContext.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
		return source.StatefulSets()
	}
	return nil
}

func (v snapshotReadView) HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler {
	if source, ok := v.DetectionContext.(interface {
		HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	}); ok {
		return source.HorizontalPodAutoscalers()
	}
	return nil
}

func (v snapshotReadView) PodDisruptionBudgets() []policyv1.PodDisruptionBudget {
	if source, ok := v.DetectionContext.(interface {
		PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	}); ok {
		return source.PodDisruptionBudgets()
	}
	return nil
}

func (v snapshotReadView) ObservedAt() time.Time {
	if clock, ok := v.DetectionContext.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v snapshotReadView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if source, ok := v.DetectionContext.(servicehealth.ReachabilitySource); ok {
		return source.ServiceReachability()
	}
	return servicehealth.ReachabilityWindow{}, false
}
